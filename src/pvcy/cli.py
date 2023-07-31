# Copyright 2023 Privacy Dynamics, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import shutil
from pathlib import Path
from typing import List, Tuple, TypeVar, Union
from uuid import UUID

import click

from pvcy.client import PvcyClient
from pvcy.project_types import ConfigFile, NewJobDefinition, ProjectConfig
from pvcy.yaml import get_yaml_serializer

T = TypeVar("T", NewJobDefinition, ProjectConfig)
CONFIG_FILE_VERSION = 1


def _build_client_from_ctx(ctx: click.Context) -> PvcyClient:
    assert ctx.parent, "Error! Can only build a client from a subcommand."
    return PvcyClient(
        base_url=ctx.parent.params.get("base_url"),
        access_token=ctx.parent.params.get("access_token"),
        client_id=ctx.parent.params.get("client_id"),
        client_secret=ctx.parent.params.get("client_secret"),
        audience=ctx.parent.params.get("audience"),
        idp_domain=ctx.parent.params.get("idp_domain"),
        raise_on_http_error=bool(ctx.parent.params.get("raise_on_http_error")),
    )


def _build_project_configs_from_api_state(client: PvcyClient) -> List[ProjectConfig]:
    configs: List[ProjectConfig] = []
    projects = client.get_projects()
    for p in projects:
        job_defs = client.get_job_definitions(p.project_id)
        jd_schemas = [NewJobDefinition.from_job_definition(jd) for jd in job_defs]
        configs.append(
            ProjectConfig.from_project_and_job_defs(
                project=p, job_definitions=jd_schemas
            )
        )
    return configs


def _diff_lists(
    config: List[T], state: List[T], attr: str
) -> Tuple[List[T], List[T], List[T]]:
    """
    Returns:
        Tuple[List[ProjectConfig], List[ProjectConfig]]: First element is a list of
        objects in the config file not found in the state; second element is a list
        of objects in the state not in the config file. Third element is a list
        of intersecting objects (from the CONFIG).
    """
    config_ids = {getattr(i, attr) for i in config}
    state_ids = {getattr(i, attr) for i in state}
    only_state_ids = state_ids - config_ids
    if only_state_ids:
        logging.debug(
            f"{attr}s found in state missing from config file: {only_state_ids}"
        )
    only_state = [i for i in state if getattr(i, attr) in only_state_ids]
    only_cfg_ids = config_ids - state_ids
    if only_cfg_ids:
        logging.debug(f"{attr}s found in cfg file missing from state: {only_cfg_ids}")
    only_cfg = [
        i
        for i in config
        if getattr(i, attr) in only_cfg_ids or getattr(i, attr) is None
    ]
    intersection = [i for i in config if getattr(i, attr) in state_ids & config_ids]
    return only_cfg, only_state, intersection


def _merge_job_definitions(
    config: List[NewJobDefinition],
    state: List[NewJobDefinition],
    attr: str,
) -> List[NewJobDefinition]:
    only_cfg, only_state, intersection = _diff_lists(config, state, attr=attr)
    for job_definition in intersection:
        logging.debug(
            f"Merging config for job def with {attr} {getattr(job_definition, attr)}"
        )
        [job_definition_state] = [
            jd for jd in state if getattr(jd, attr) == getattr(job_definition, attr)
        ]
        if (
            not job_definition.job_definition_id
            and job_definition_state.job_definition_id
        ):
            job_definition.job_definition_id = job_definition_state.job_definition_id

        if job_definition_state.column_config and not job_definition.column_config:
            logging.debug(
                "Using column config found in state for job def: "
                f"{job_definition.job_definition_id}"
            )
            job_definition.column_config = job_definition_state.column_config.copy()
        elif job_definition_state.column_config and job_definition.column_config:
            logging.debug(
                "Updating column config with values from state for job def: "
                f"{job_definition.job_definition_id}"
            )
            merged = job_definition_state.column_config.copy()
            merged.update(job_definition.column_config)
            job_definition.column_config = merged
        else:
            logging.debug(
                "No column config found in state; using column config from file for "
                f"job def {job_definition.job_definition_id}"
            )
    return sorted(
        [*only_cfg, *only_state, *intersection],
        key=lambda x: (x.table_name, x.job_definition_id),
    )


def _validate_file_path(_, __, value: Union[Path, None]) -> Path:  # type: ignore
    if value is None:
        value = Path.cwd() / "pvcy_cfg.yml"
    elif value.suffix not in (".yml", ".yaml"):
        raise click.BadParameter("Can only export to .yml or .yaml file")
    return value


@click.group
@click.option(
    "--base_url",
    help=(
        "The base URL for all API calls. Can be provided as the "
        "environment variable PVCY_BASE_URL instead of being passed into the "
        "CLI. Defaults to https://api.privacydynamics.io."
    ),
)
@click.option(
    "--access_token",
    help=(
        "An OAuth2 Bearer Token for authenticating requests. "
        "If not provided, when the client is initialized, the token "
        "will be fetched using the client_id and client_secret from the "
        "idp_domain."
    ),
)
@click.option(
    "--client_id",
    help=(
        "The client_id for an OAuth2 client-credentials "
        "flow. Can be provided as the environment variable PVCY_CLIENT_ID "
        "instead of being passed into the CLI."
    ),
)
@click.option(
    "--client_secret",
    help=(
        "The client_secret for an OAuth2 client-credentials "
        "flow. Can be provided as the environment variable PVCY_CLIENT_SECRET "
        "instead of being passed into the CLI."
    ),
)
@click.option(
    "--audience",
    help=(
        "The identity claim's intended audience for an OAuth2 "
        "client-credentials flow. Can be provided as the environment variable "
        "PVCY_AUDIENCE instead of being passed into the CLI. Defaults "
        "to https://api.privacydynamics.io."
    ),
)
@click.option(
    "--idp_domain",
    help=(
        "The identity provider domain for an OAuth2 "
        "client-credentials flow. Can be provided as the environment variable "
        "PVCY_IDP_DOMAIN instead of being passed into the CLI. "
        "Defaults to https://auth.privacydynamics.io."
    ),
)
@click.option(
    "--raise_on_http_error",
    type=bool,
    default=True,
    help=("Defaults to True; if False, errors are logged and responses are returned."),
)
@click.option(
    "--debug",
    "-d",
    is_flag=True,
    help=("Show all debug logs from API calls and other libraries."),
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help=("Show more information while the tool is processing the operations."),
)
@click.pass_context
def pvcy(
    ctx: click.Context,
    base_url: str,
    access_token: str,
    client_id: str,
    client_secret: str,
    audience: str,
    idp_domain: str,
    raise_on_http_error: bool,
    debug: bool,
    verbose: bool,
) -> None:
    if debug:
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
    elif verbose:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)


@pvcy.command()
@click.pass_context
@click.argument(
    "path",
    required=False,
    type=click.Path(
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
        resolve_path=True,
        path_type=Path,
    ),
    callback=_validate_file_path,
)
@click.option(
    "--overwrite-existing",
    "-y",
    is_flag=True,
    help="Skip the user prompt before overwriting an existing file.",
)
def export(ctx: click.Context, path: Path, overwrite_existing: bool) -> None:
    """
    This command exports the current configuration from the API to
    a YAML file.

    The program exits with code 0 if a file was exported successfully.
    It exits with code 1 or 2 or raises an exception for errors.

    Args:
        path (Path): The file path to export to. Defaults to `./pvcy_cfg.yml`.
        NOTE: This completely overwrites an existing file at `path`.
    """
    if not path.exists() or click.confirm(f"Overwrite existing file at {path}?"):
        logging.info("Building and authenticating PVCY client.")
        pvcy_client = _build_client_from_ctx(ctx)
        logging.info("Fetching project and dataset configs.")
        projects = _build_project_configs_from_api_state(client=pvcy_client)

        logging.info(f"Dumping YAML to {path}")
        projects = sorted(projects, key=lambda x: (x.project_title, x.project_id))
        config = ConfigFile(version=CONFIG_FILE_VERSION, projects=projects)
        yaml_serializer = get_yaml_serializer()
        with open(path, "w") as f:
            yaml_serializer.dump(config.model_dump(mode="json"), f)
        ctx.exit(0)
    else:
        logging.error(f"Aborted export due to existing file at {path}.")
        ctx.exit(1)


@pvcy.command()
@click.pass_context
@click.argument(
    "path",
    required=False,
    type=click.Path(
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
        resolve_path=True,
        path_type=Path,
    ),
    callback=_validate_file_path,
)
@click.option(
    "--only",
    "-o",
    multiple=True,
    help=(
        "A space-separated list of project IDs to sync. Other "
        "projects will remain in the config file but not have "
        "state synced from the API and will not have their config "
        "pushed to the API."
    ),
)
def sync(ctx: click.Context, path: Path, only: List[str]) -> None:
    """
    This command does the following:
        1.  Loads a YML config file at `path`.
        2.  Fetches the latest account state from the PVCY API.
        3.  Merges the config file with the current state (the config
            file takes precedence in any conflicts).
        4.  Updates the account state to match the merged config file.
        5.  Writes the final config back to the file at `path`.

    NOTE: It is not possible to delete projects or job definitions using
    this program; sync assumes that projects found in the API and not
    in the config file should be written to the config file; it also
    assumes that job definitions found in the API or config file not in
    the other should be created and written to the config file. Resources
    must be deleted in both the UI and the config file BEFORE running sync.

    The program exits with code 0 if all of the operations above complete
    successfully. It exits with code 1 or 2 or raises an exception for errors.

    Args:
        path (Path): The file path to export to. Defaults to `./pvcy_cfg.yml`.
    """
    yaml_serializer = get_yaml_serializer()
    if not path.exists():
        logging.warning(
            f"No config file was found at {path}. Exporting config from the API only."
        )
        config = ConfigFile(version=CONFIG_FILE_VERSION, projects=[])
    else:
        logging.info(f"Loading config from file at {path}.")
        raw_config = yaml_serializer.load(path)
        config = ConfigFile(**raw_config)

    logging.info("Building and authenticating PVCY client.")
    pvcy_client = _build_client_from_ctx(ctx)
    logging.info("Fetching project and dataset state.")
    state = _build_project_configs_from_api_state(client=pvcy_client)
    only_sync_ids = [UUID(s) for s in only]
    if only_sync_ids:
        state = [p for p in state if p.project_id in only_sync_ids]

    if only and len(state) < len(only):
        logging.error(
            "--only option provided with Project ID that could not be found in the "
            "API. Found the following matching projects: "
            f"{[p.project_id for p in state]}"
        )
        ctx.exit(2)

    logging.info("Merging configs with state.")
    only_cfg_projects, only_state_projects, intersection_projects = _diff_lists(
        config.projects, state, attr="project_id"
    )
    # Note: Cannot currently add projects through a the API; detect projects
    # added in the config file and make a backup.
    if only_cfg_projects and not only_sync_ids:
        backup_path = path.with_stem(path.stem + "__backup")
        logging.warning(
            "Found config for projects that could not be found in the API. "
            "This tool does not support creating new projects. Create a project "
            "in the UI first. (You can add and configure datasets using this tool). "
            "These Project IDs will be dropped from the merged file: "
            f"{[p.project_id for p in only_cfg_projects]}. "
            f"Creating backup of original file at {backup_path}."
        )
        shutil.copyfile(path, backup_path)

    merged: List[ProjectConfig] = only_state_projects
    if only_sync_ids:
        merged.extend(only_cfg_projects)
    for project in intersection_projects:
        [project_state] = [p for p in state if p.project_id == project.project_id]
        if project_state.job_definitions and not project.job_definitions:
            project.job_definitions = project_state.job_definitions.copy()
        elif project_state.job_definitions and project.job_definitions:
            project.job_definitions = _merge_job_definitions(
                project.job_definitions,
                project_state.job_definitions,
                attr="job_definition_id",
            )
        merged.append(project)

    if config.projects and merged:
        logging.info("Updating account state from config file.")
        # can't create projects or update project metadata; just update job definitions
        for project in merged:
            if only_sync_ids and project.project_id not in only_sync_ids:
                continue
            # todo: check to see if project was changed; skip API update if
            # it is the same
            newly_updated = pvcy_client.create_or_update_job_definitions(
                project_id=project.project_id, job_definitions=project.job_definitions
            )
            project.job_definitions = [
                NewJobDefinition.from_job_definition(jd) for jd in newly_updated
            ]

    logging.info(f"Writing merged config to {path}")
    # TODO: a deep update on raw_config should preserve original order and comments
    merged = sorted(merged, key=lambda x: (x.project_title, x.project_id))
    new_config = ConfigFile(version=CONFIG_FILE_VERSION, projects=merged)
    with open(path, "w") as f:
        yaml_serializer.dump(new_config.model_dump(mode="json"), f)
    ctx.exit(0)


@pvcy.command()
@click.pass_context
@click.argument(
    "path",
    required=False,
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=True,
        resolve_path=True,
        path_type=Path,
    ),
    callback=_validate_file_path,
)
@click.argument("source", nargs=1)
@click.argument("destination", nargs=-1)
def copy(ctx: click.Context, path: Path, source: str, destination: List[str]) -> None:
    """
    This command does the following:
        1.  Loads a YML config file at `path`.
        2.  Merges the config for the project with an ID matching `source`
            with the config for the projects with IDs in the `destination`
            list. The source config takes precedence in any conflicts.
        3.  Writes the merged config file back to `path`.

    NOTE: This program does not interact with the PVCY API. You may wish
    to run `pvcy sync` before and after `pvcy copy ...`. You cannot create
    new projects with `pvcy copy`.

    The program exits with code 0 if all of the operations above complete
    successfully. It exits with code 1 or 2 or raises an exception for errors.

    Args:
        path (Path): The file path to export to. Defaults to `./pvcy_cfg.yml`.
        source (str): The UUID for the project to copy config FROM.
        destination (List[str]): The UUID(s) for one or more projects to copy config
            TO. Note that job definitions are merged using the origin table
            names.
    """
    yaml_serializer = get_yaml_serializer()
    if not destination:
        logging.error(
            "You must provide one or more desination project IDs to copy config to."
        )
        ctx.exit(1)

    logging.info(f"Loading config from file at {path}.")
    raw_config = yaml_serializer.load(path)
    config = ConfigFile(**raw_config)
    source_uuid = UUID(source)
    destination_uuids = [UUID(d) for d in destination]

    project_ids = [p.project_id for p in config.projects]
    logging.debug(f"Found config for projects: {project_ids}")
    if source_uuid not in project_ids:
        logging.error(
            f"No config was found at {path} for the project with ID: {source_uuid}. "
            f"Cannot copy nonexistent config. Check for typos, or "
            f"run `pvcy export` or `pvcy sync` first."
        )
        ctx.exit(1)
    else:
        # load the source project, but delete the job definition IDs,
        # since those shouldn't be copied to the destination projects.
        [source_proj] = [
            p.model_copy(deep=True)
            for p in config.projects
            if p.project_id == source_uuid
        ]
        for jd in source_proj.job_definitions:
            jd.job_definition_id = None

    for dest_id in destination_uuids:
        if dest_id not in project_ids:
            logging.error(
                f"No config was found at {path} for the project with ID: {dest_id}. "
                "Cannot copy nonexistent config. Check for typos, or "
                "run `pvcy export` or `pvcy sync` first. You cannot use pvcy copy "
                "to create a new project."
            )
            ctx.exit(1)

    dest_projects = [p for p in config.projects if p.project_id in destination_uuids]
    logging.debug(f"Overwriting config for these projects: {dest_projects}")
    unchanged_projects = [
        p for p in config.projects if p.project_id not in destination_uuids
    ]
    for dest_proj in dest_projects:
        logging.info(f"Copying config from {source_uuid} to {dest_proj.project_id}.")
        if source_proj.job_definitions and not dest_proj.job_definitions:
            dest_proj.job_definitions = source_proj.job_definitions
        elif dest_proj.job_definitions and source_proj.job_definitions:
            dest_proj.job_definitions = _merge_job_definitions(
                [jd.model_copy(deep=True) for jd in source_proj.job_definitions],
                dest_proj.job_definitions,
                attr="table_name",
            )

    logging.info(f"Writing merged config to {path}")
    merged = sorted(
        [*unchanged_projects, *dest_projects],
        key=lambda x: (x.project_title, x.project_id),
    )
    new_config = ConfigFile(version=CONFIG_FILE_VERSION, projects=merged)
    # TODO: a deep update on raw_config should preserve original order and comments
    with open(path, "w") as f:
        yaml_serializer.dump(new_config.model_dump(mode="json"), f)

    ctx.exit(0)
