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

import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, TypeVar, Union
from uuid import uuid4

import pytest
import yaml
from click.testing import CliRunner
from pvcy.cli import (
    _build_project_configs_from_api_state,
    _diff_lists,
    _merge_job_definitions,
    pvcy,
)
from pvcy.client import PvcyClient
from pvcy.project_types import (
    ColumnConfig,
    ConfigFile,
    JobRunType,
    KStrategy,
    NewJobDefinition,
    PiiClass,
    Project,
    ProjectConfig,
    TreatmentMethod,
)
from pydantic import BaseModel

from .utils import load_response_from_file


@pytest.fixture
def test_data_dir() -> Path:
    return Path(__file__).parent.parent / "data"


class MockPvcyClient(PvcyClient):
    def __post_init__(self) -> None:
        pass

    def _get(self, endpoint: str) -> Dict[str, Any]:
        if endpoint == "/v1/projects":
            return load_response_from_file("responses/get_projects.json")
        elif endpoint == "/v1/connections":
            return load_response_from_file("responses/get_connections.json")
        elif re.match(r"/v1/projects/([a-f0-9-]{36})/job_definitions", endpoint):
            return load_response_from_file("responses/get_job_definitions.json")
        else:
            raise NotImplementedError("Mock endpoint not implemented")

    def _post(
        self, endpoint: str, json_data: Any, params: Union[Dict[str, Any], None] = None
    ) -> Dict[str, Any]:
        if endpoint == "/v1/projects":
            return load_response_from_file("responses/get_project.json")
        elif endpoint == "/v1/connections":
            return load_response_from_file("responses/get_connection.json")
        elif re.match(r"/v1/projects/([a-f0-9-]{36})/job_definitions", endpoint):
            return load_response_from_file("responses/get_job_definition.json")
        elif re.match(
            r"/v1/projects/([a-f0-9-]{36})/job_definitions:batch_create", endpoint
        ):
            return load_response_from_file("responses/get_job_definitions.json")
        else:
            raise NotImplementedError("Mock endpoint not implemented")

    def _put(self, endpoint: str, json_data: Any) -> Dict[str, Any]:
        if re.match(r"/v1/projects/([a-f0-9-]{36})/job_definitions", endpoint):
            return load_response_from_file("responses/get_job_definition.json")
        else:
            raise NotImplementedError(
                f"Mock endpoint not implemented for _put {endpoint}"
            )

    def _delete(self, endpoint: str) -> None:
        raise NotImplementedError(
            f"Mock endpoint not implemented for _delete {endpoint}"
        )


@pytest.fixture
def mock_pvcy_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pvcy.cli.PvcyClient", MockPvcyClient)


@pytest.fixture
def job_definitions() -> List[NewJobDefinition]:
    return [
        NewJobDefinition(
            table_name="foo",
            job_definition_id=uuid4(),
            enabled=True,
            job_run_type=JobRunType.LOAD_TREAT_WRITE,
            k_strategy=KStrategy.ANONYMIZE,
            k_target=2,
            schedule=None,
            column_config={
                "name": ColumnConfig(
                    treatment_method=TreatmentMethod.FAKE, pii_class=PiiClass.FULL_NAME
                ),
                "email": ColumnConfig(
                    treatment_method=TreatmentMethod.HASH,
                    pii_class=PiiClass.EMAIL_ADDRESS,
                ),
                "ltv": ColumnConfig(
                    treatment_method=TreatmentMethod.LOCK, pii_class=None
                ),
            },
        ),
        NewJobDefinition(
            table_name="bar",
            job_definition_id=uuid4(),
            enabled=True,
            job_run_type=JobRunType.LOAD_TREAT_WRITE,
            k_strategy=KStrategy.ANONYMIZE,
            k_target=2,
            schedule=None,
            column_config={
                "name": ColumnConfig(
                    treatment_method=TreatmentMethod.FAKE, pii_class=PiiClass.FULL_NAME
                ),
                "phone": ColumnConfig(
                    treatment_method=TreatmentMethod.HASH,
                    pii_class=PiiClass.PHONE_NUMBER,
                ),
                "num_engagements": ColumnConfig(
                    treatment_method=TreatmentMethod.LOCK, pii_class=None
                ),
            },
        ),
        NewJobDefinition(
            table_name="baz",
            job_definition_id=uuid4(),
            enabled=True,
            job_run_type=JobRunType.LOAD_TREAT_WRITE,
            k_strategy=KStrategy.ANONYMIZE,
            k_target=2,
            schedule=None,
            column_config={
                "id": ColumnConfig(
                    treatment_method=TreatmentMethod.LOCK, pii_class=None
                ),
                "value": ColumnConfig(
                    treatment_method=TreatmentMethod.LOCK, pii_class=None
                ),
            },
        ),
    ]


T = TypeVar("T", BaseModel, ProjectConfig, NewJobDefinition)


def _pydantic_deepcopy(models: List[T]) -> List[T]:
    return [m.model_copy(deep=True) for m in models]


def test_build_project_configs_from_api_state() -> None:
    client = MockPvcyClient()
    configs = _build_project_configs_from_api_state(client=client)
    assert configs
    assert isinstance(configs, list)
    assert len(configs) == 6
    assert isinstance(configs[0], ProjectConfig)
    assert all(p.job_definitions for p in configs)


def test_diff_lists() -> None:
    raw_projects = load_response_from_file("responses/get_projects.json")
    all_projects = [Project(**raw) for raw in raw_projects["projects"]]
    all_configs = [ProjectConfig.from_project(p) for p in all_projects]
    assert len(all_configs) == 6
    first_five = all_configs[:-1]
    last_one = all_configs[-1:]

    only_left, only_right, both = _diff_lists(
        all_configs, all_configs, attr="project_id"
    )
    assert only_left == []
    assert only_right == []
    assert both == all_configs

    only_left, only_right, both = _diff_lists(first_five, last_one, attr="project_id")
    assert only_left == first_five
    assert only_right == last_one
    assert both == []

    only_left, only_right, both = _diff_lists(
        all_configs, first_five, attr="project_id"
    )
    assert only_left == last_one
    assert only_right == []
    assert both == first_five


def test_merge_job_definitions_by_id(job_definitions: List[NewJobDefinition]) -> None:
    original = sorted(_pydantic_deepcopy(job_definitions), key=lambda x: x.table_name)
    left = _pydantic_deepcopy(job_definitions)
    right = _pydantic_deepcopy(job_definitions)
    merged = _merge_job_definitions(left, right, attr="job_definition_id")
    assert merged == original

    # overlapping sets become whole again
    first_two = _pydantic_deepcopy(job_definitions[:2])
    last_two = _pydantic_deepcopy(job_definitions[1:])
    merged = _merge_job_definitions(first_two, last_two, attr="job_definition_id")
    assert merged == original

    # overlapping column config becomes whole again
    left = _pydantic_deepcopy(job_definitions)
    left[0].column_config = None
    right = _pydantic_deepcopy(job_definitions)
    assert right[2].column_config
    del right[2].column_config["id"]
    merged = _merge_job_definitions(left, right, attr="job_definition_id")
    assert merged == original

    # state has different column config, which we will ignore
    left = _pydantic_deepcopy(job_definitions)
    right = _pydantic_deepcopy(job_definitions)
    assert right[0].column_config
    right[0].column_config["email"] = ColumnConfig(
        treatment_method=TreatmentMethod.FAKE,
        pii_class=PiiClass.EMAIL_ADDRESS,
    )
    merged = _merge_job_definitions(left, right, attr="job_definition_id")
    assert merged == original

    # config has different column config, which we will honor
    left = _pydantic_deepcopy(job_definitions)
    right = _pydantic_deepcopy(job_definitions)
    assert left[0].column_config
    left[0].column_config["email"] = ColumnConfig(
        treatment_method=TreatmentMethod.FAKE,
        pii_class=PiiClass.EMAIL_ADDRESS,
    )
    altered_id = left[0].job_definition_id
    merged = _merge_job_definitions(left, right, attr="job_definition_id")
    assert merged != original
    [jd] = [jd for jd in merged if jd.job_definition_id == altered_id]
    assert jd.column_config
    assert jd.column_config["email"].treatment_method == TreatmentMethod.FAKE

    # state has different jd metadata, which we will ignore
    left = _pydantic_deepcopy(job_definitions)
    right = _pydantic_deepcopy(job_definitions)
    right[0].k_target = 50
    merged = _merge_job_definitions(left, right, attr="job_definition_id")
    assert merged == original

    # config has different jd metadata, which we will honor
    left = _pydantic_deepcopy(job_definitions)
    right = _pydantic_deepcopy(job_definitions)
    left[0].k_target = 50
    altered_id = left[0].job_definition_id
    merged = _merge_job_definitions(left, right, attr="job_definition_id")
    assert merged != original
    [jd] = [jd for jd in merged if jd.job_definition_id == altered_id]
    assert jd.k_target == 50


def test_merge_job_definitions_by_table_name(
    job_definitions: List[NewJobDefinition],
) -> None:
    original = sorted(_pydantic_deepcopy(job_definitions), key=lambda x: x.table_name)
    left = _pydantic_deepcopy(job_definitions)
    right = _pydantic_deepcopy(job_definitions)
    for jd in right:
        jd.job_definition_id = uuid4()
    merged = _merge_job_definitions(left, right, attr="table_name")
    assert merged == original

    # config is missing one
    left = _pydantic_deepcopy(job_definitions)[1:]
    right = _pydantic_deepcopy(job_definitions)
    for jd in right:
        jd.job_definition_id = uuid4()
    merged = _merge_job_definitions(left, right, attr="table_name")
    assert merged != original  # diff uuid for foo
    assert len(merged) == len(original)
    assert all([m.table_name == o.table_name for m, o in zip(merged, original)])

    # state is missing one
    left = _pydantic_deepcopy(job_definitions)
    right = _pydantic_deepcopy(job_definitions)[1:]
    for jd in right:
        jd.job_definition_id = uuid4()
    merged = _merge_job_definitions(left, right, attr="table_name")
    assert merged == original


def test_export(mock_pvcy_client: None, tmp_path: Path, test_data_dir: Path) -> None:
    p = tmp_path / "export.yml"
    runner = CliRunner()
    result = runner.invoke(pvcy, args=["export", str(p.as_posix())])
    assert result.exit_code == 0
    with open(p, "r") as f:
        raw_contents = f.read()
        f.seek(0)
        contents = yaml.safe_load(f)

    with open(test_data_dir / "config" / "exported.yml") as f:
        expected_raw_contents = f.read()

    try:
        config = ConfigFile(**contents)
        for proj in config.projects:
            assert proj.project_id
            assert proj.job_definitions
        assert raw_contents == expected_raw_contents
    except AssertionError as e:
        print(raw_contents)
        raise e from None


def test_sync_noop(mock_pvcy_client: None, tmp_path: Path, test_data_dir: Path) -> None:
    test_file = test_data_dir / "config" / "exported.yml"
    with open(test_file, "r") as f:
        expected_contents = f.read()

    p = tmp_path / "sync.yml"
    shutil.copy(test_file, p)
    runner = CliRunner()
    result = runner.invoke(pvcy, args=["sync", str(p.as_posix())])
    assert result.exit_code == 0
    with open(p, "r") as f:
        raw_contents = f.read()

    assert raw_contents == expected_contents


def test_sync_missing_partial(
    mock_pvcy_client: None, tmp_path: Path, test_data_dir: Path
) -> None:
    test_file = test_data_dir / "config" / "missing_first_project.yml"
    expected_file = test_data_dir / "config" / "exported.yml"
    with open(expected_file, "r") as f:
        expected_contents = f.read()

    p = tmp_path / "sync.yml"
    shutil.copy(test_file, p)
    runner = CliRunner()
    result = runner.invoke(pvcy, args=["sync", str(p.as_posix())])
    assert result.exit_code == 0
    with open(p, "r") as f:
        raw_contents = f.read()

    assert raw_contents == expected_contents


def test_sync_new_job_defs(
    mock_pvcy_client: None, tmp_path: Path, test_data_dir: Path
) -> None:
    test_file = test_data_dir / "config" / "different_first_project.yml"
    exported_file = test_data_dir / "config" / "exported.yml"

    p = tmp_path / "sync.yml"
    shutil.copy(test_file, p)
    runner = CliRunner()
    result = runner.invoke(pvcy, args=["sync", str(p.as_posix())])
    assert result.exit_code == 0
    with open(p, "r") as f:
        contents = yaml.safe_load(f)
    with open(test_file, "r") as f:
        test_file_contents = yaml.safe_load(f)
    with open(exported_file, "r") as f:
        exported_contents = yaml.safe_load(f)

    actual_config = ConfigFile(**contents)
    test_file_config = ConfigFile(**test_file_contents)
    exported_config = ConfigFile(**exported_contents)

    assert actual_config.projects[0] == test_file_config.projects[0]
    assert (
        actual_config.projects[0].project_id == exported_config.projects[0].project_id
    )
    assert actual_config.projects[0] != exported_config.projects[0]
    assert actual_config.projects[1:] == exported_config.projects[1:]


# todo: test --only
# todo: test new projects (cfg only warning)


def test_copy(mock_pvcy_client: None, tmp_path: Path, test_data_dir: Path) -> None:
    test_file = test_data_dir / "config" / "for_copying.yml"
    with open(test_file, "r") as f:
        test_file_contents = yaml.safe_load(f)

    p = tmp_path / "copy.yml"
    shutil.copy(test_file, p)
    runner = CliRunner()
    source_id = "c132b205-4d88-47c8-af4d-884d8f17df15"
    dest_ids = [
        "47332635-c555-496a-b20b-303914e9230c",
        "105d0358-cb82-4549-a93d-33d436a6118b",
    ]
    result = runner.invoke(pvcy, args=["copy", str(p.as_posix()), source_id, *dest_ids])
    assert result.exit_code == 0

    with open(p, "r") as f:
        raw_contents = f.read()
        print(raw_contents)
        f.seek(0)
        contents = yaml.safe_load(f)

    actual_config = ConfigFile(**contents)
    test_file_config = ConfigFile(**test_file_contents)

    assert len(actual_config.projects) == len(test_file_config.projects)

    actual_project_ids = [str(p.project_id) for p in actual_config.projects]
    assert source_id in actual_project_ids
    assert all([dest_id in actual_project_ids for dest_id in dest_ids])

    [source_project] = [
        p for p in actual_config.projects if str(p.project_id) == source_id
    ]
    assert [source_project] == [
        p for p in test_file_config.projects if str(p.project_id) == source_id
    ]
    dest_projects = [p for p in actual_config.projects if str(p.project_id) in dest_ids]
    assert dest_projects != [
        p for p in test_file_config.projects if str(p.project_id) in dest_ids
    ]

    for dest_proj in dest_projects:
        assert len(dest_proj.job_definitions) >= len(source_project.job_definitions)
        [orig_config] = [
            p for p in test_file_config.projects if p.project_id == dest_proj.project_id
        ]
        orig_config_table_names = [jd.table_name for jd in orig_config.job_definitions]
        source_proj_table_names = [
            jd.table_name for jd in source_project.job_definitions
        ]
        for dest_jd in dest_proj.job_definitions:
            if dest_jd.table_name in orig_config_table_names:
                assert dest_jd.job_definition_id is not None
            else:
                assert dest_jd.job_definition_id is None

            if dest_jd.table_name in source_proj_table_names:
                [source_jd] = [
                    jd
                    for jd in source_project.job_definitions
                    if jd.table_name == dest_jd.table_name
                ]
                assert dest_jd.column_config == source_jd.column_config
                assert dest_jd.k_target == source_jd.k_target
                assert dest_jd.schedule == source_jd.schedule


# todo: test project id validation in copy.
@pytest.mark.parametrize(
    "bad_params",
    [
        (["c132b205-4d88-47c8-af4d-884d8f17df15"]),
        (
            [
                "12345678-4d88-47c8-af4d-884d8f17df15",
                "47332635-c555-496a-b20b-303914e9230c",
            ]
        ),
        (
            [
                "c132b205-4d88-47c8-af4d-884d8f17df15",
                "12345678-c555-496a-b20b-303914e9230c",
            ]
        ),
        (["file.yml", "c132b205-4d88-47c8-af4d-884d8f17df15"]),
        (
            [
                "file.yml",
                "c132b205-4d88-47c8-af4d-884d8f17df15",
                "47332635-c555-496a-b20b-303914e9230c",
            ]
        ),
    ],
)
def test_copy_bad_params(
    mock_pvcy_client: None, tmp_path: Path, test_data_dir: Path, bad_params: List[str]
) -> None:
    test_file = test_data_dir / "config" / "for_copying.yml"
    p = tmp_path / "copy.yml"
    shutil.copy(test_file, p)
    runner = CliRunner()
    result = runner.invoke(pvcy, args=["copy", *bad_params])
    assert result.exit_code == 2
    assert "error" in result.stdout.lower()
