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

from pvcy.connection_types import connection_factory
from pvcy.project_types import (
    JobDefinition,
    NewJobDefinition,
    Project,
    ProjectConfig,
)

from .utils import load_response_from_file


def test_connections_validate() -> None:
    response = load_response_from_file("responses/get_connections.json")
    connections = response["connections"]
    for c in connections:
        connection_factory(c)


def test_projects_validate() -> None:
    response = load_response_from_file("responses/get_projects.json")
    projects = response["projects"]
    for p in projects:
        Project(**p)


def test_project_validates() -> None:
    response = load_response_from_file("responses/get_project.json")
    project = response["project"]
    Project(**project)


def test_job_definition_validates() -> None:
    response = load_response_from_file("responses/get_job_definition.json")
    jd = response["job_definition"]
    JobDefinition(**jd)


def test_new_job_definition_from_job_definition() -> None:
    response = load_response_from_file("responses/get_job_definition.json")
    jd = response["job_definition"]
    job_definition = JobDefinition(**jd)
    new_jd = NewJobDefinition.from_job_definition(job_definition)
    assert new_jd.job_definition_id == job_definition.job_definition_id
    new_jd_no_id = NewJobDefinition.from_job_definition(
        job_definition=job_definition, keep_id=False
    )
    assert new_jd_no_id.job_definition_id is None


def test_project_config_from_project() -> None:
    response = load_response_from_file("responses/get_project.json")
    project = Project(**response["project"])
    config = ProjectConfig.from_project(project=project)
    assert config


def test_project_config_from_project_and_job_defs() -> None:
    p_response = load_response_from_file("responses/get_project.json")
    project = Project(**p_response["project"])
    jd_response = load_response_from_file("responses/get_job_definitions.json")
    job_definitions = [JobDefinition(**jd) for jd in jd_response["job_definitions"]]
    jd_schemas = [NewJobDefinition.from_job_definition(jd) for jd in job_definitions]
    config = ProjectConfig.from_project_and_job_defs(
        project=project, job_definitions=jd_schemas
    )
    assert config
    assert config.job_definitions
