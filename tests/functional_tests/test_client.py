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

import os
from datetime import datetime
from typing import Any, Dict, Iterator, List, Tuple

import pytest
from pvcy.client import PvcyClient
from pvcy.connection_types import (
    Connection,
    NewSnowflakeConnection,
    SnowflakeConnection,
)
from pvcy.project_types import (
    ColumnConfig,
    JobDefinition,
    JobRunType,
    KStrategy,
    NewJobDefinition,
    PiiClass,
    Project,
    TreatmentMethod,
)
from requests import HTTPError

TEST_PROJECT_ID = os.getenv("TEST_PROJECT_ID", "87d3f199-4f74-4062-88ad-3c75cb406501")


@pytest.fixture(scope="session")
def pvcy_client() -> PvcyClient:
    return PvcyClient()


@pytest.fixture(scope="session")
def snowflake_creds() -> Tuple[str, str]:
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    assert account, "You must set the SNOWFLAKE_ACCOUNT env var for this test."
    password = os.getenv("SNOWFLAKE_PASSWORD")
    assert password, "You must set the SNOWFLAKE_PASSWORD env var for this test."
    return account, password


@pytest.fixture
def snowflake_connection(
    pvcy_client: PvcyClient, snowflake_creds: Tuple[str, str]
) -> Iterator[Connection]:
    name = f"API Test Connection {datetime.utcnow()}"
    new_conn = NewSnowflakeConnection(
        connection_name=name,
        account=snowflake_creds[0],
        username="SVC_PVCY",
        password=snowflake_creds[1],
        database="RAW_SENSITIVE",
    )
    conn = pvcy_client.create_connection(new_conn)
    yield conn
    pvcy_client.delete_connection(conn.connection_id)


def test_get_projects(pvcy_client: PvcyClient) -> None:
    projects = pvcy_client.get_projects()

    assert projects
    assert isinstance(projects, list)
    assert len(projects) > 1

    first_project = projects[0]
    assert isinstance(first_project, Project)

    p = pvcy_client.get_project(first_project.project_id)
    assert p == first_project


def test_get_connections(pvcy_client: PvcyClient) -> None:
    connections = pvcy_client.get_connections()

    assert connections
    assert isinstance(connections, list)
    assert len(connections) > 1


def test_update_connection_snowflake(
    pvcy_client: PvcyClient,
    snowflake_connection: SnowflakeConnection,
    snowflake_creds: Tuple[str, str],
) -> None:
    new_connection_name = f"API Test Source {datetime.utcnow()}"
    conn = NewSnowflakeConnection(
        connection_name=new_connection_name,
        account=snowflake_creds[0],
        username="SVC_PVCY",
        password=snowflake_creds[1],
        database="RAW_SENSITIVE",
    )

    patched_conn = pvcy_client.update_connection(
        snowflake_connection.connection_id, conn
    )
    assert isinstance(patched_conn, SnowflakeConnection)
    assert patched_conn.connection_id == snowflake_connection.connection_id
    assert patched_conn.connection_name == new_connection_name


def test_test_connection(
    pvcy_client: PvcyClient, snowflake_connection: SnowflakeConnection
) -> None:
    works = pvcy_client.test_connection(snowflake_connection.connection_id)
    assert works


# @pytest.mark.skip(reason="Endpoint not yet public.")
def test_get_job_definitions(pvcy_client: PvcyClient) -> None:
    job_definitions = pvcy_client.get_job_definitions(TEST_PROJECT_ID)
    assert isinstance(job_definitions, list)
    assert len(job_definitions) > 1

    first_jd = job_definitions[0]
    assert isinstance(first_jd, JobDefinition)

    p = pvcy_client.get_job_definition(
        project_id=TEST_PROJECT_ID,
        job_definition_id=first_jd.job_definition_id,
    )
    assert p == first_jd


@pytest.fixture
def job_definition(pvcy_client: PvcyClient) -> Iterator[JobDefinition]:
    new_job_definition = NewJobDefinition(
        table_name="foo",
        job_run_type=JobRunType.LOAD_TREAT_WRITE,
        k_target=2,
        k_strategy=KStrategy.ANONYMIZE,
        column_config={
            "email": ColumnConfig(
                treatment_method=TreatmentMethod.FAKE, pii_class=PiiClass.EMAIL_ADDRESS
            ),
            "name": ColumnConfig(
                treatment_method=TreatmentMethod.HIDE, pii_class=PiiClass.FULL_NAME
            ),
        },
    )
    jd = pvcy_client.create_job_definition(
        project_id=TEST_PROJECT_ID, job_definition=new_job_definition
    )
    yield jd
    try:
        pvcy_client.delete_job_definition(
            project_id=TEST_PROJECT_ID, job_definition_id=jd.job_definition_id
        )
    except HTTPError:
        pass


def test_update_job_definition(
    pvcy_client: PvcyClient, job_definition: JobDefinition
) -> None:
    patch = NewJobDefinition.from_job_definition(
        job_definition.model_copy(update={"k_target": 50})
    )
    assert job_definition.k_target is not None
    assert patch.k_target is not None
    assert job_definition.k_target < patch.k_target
    updated_jd = pvcy_client.update_job_definition(
        project_id=TEST_PROJECT_ID,
        job_definition_id=job_definition.job_definition_id,
        job_definition=patch,
    )
    assert updated_jd.k_target == patch.k_target


def test_create_or_update_job_definition(
    pvcy_client: PvcyClient, job_definition: JobDefinition
) -> None:
    patched_foo = NewJobDefinition.from_job_definition(
        job_definition.model_copy(update={"k_target": 50})
    )
    bar = NewJobDefinition(
        table_name="bar", job_run_type=JobRunType.LOAD_ASSESS, column_config={}
    )
    baz = NewJobDefinition(
        table_name="baz", job_run_type=JobRunType.LOAD_WRITE, column_config={}
    )
    jds = pvcy_client.create_or_update_job_definitions(
        project_id=TEST_PROJECT_ID, job_definitions=[patched_foo, bar, baz]
    )
    assert jds
    try:
        table_names = [jd.origin_table for jd in jds]
        assert all([n in table_names for n in ["foo", "bar", "baz"]])
    except Exception as e:
        raise e from None
    finally:
        ids_to_delete = [jd.job_definition_id for jd in jds]
        for jd_id in ids_to_delete:
            try:
                pvcy_client.delete_job_definition(
                    project_id=TEST_PROJECT_ID, job_definition_id=jd_id
                )
            except Exception:
                pass


@pytest.fixture
def pii_records() -> List[Dict[str, Any]]:
    records = [
        {
            "id": "1234567",
            "first_name": "Ted",
            "email": "ted@example.com",
            "age": 27,
            "favorite_color": "green",
        },
        {
            "id": "7654321",
            "first_name": "Bob",
            "email": "bob@example.com",
            "age": 47,
            "favorite_color": "red",
        },
    ]
    return records


def test_pseudonymize_records_redact(
    pvcy_client: PvcyClient, pii_records: List[Dict[str, Any]]
) -> None:
    treated_records = pvcy_client.pseudonymize_records(
        records=pii_records, suppresssion_method="hide", exclude_untreated=False
    )
    assert treated_records
    assert len(treated_records) == 2

    assert all(
        [
            sorted(list(record.keys())) == ["age", "favorite_color", "id"]
            for record in treated_records
        ]
    )
    [ted] = [record for record in treated_records if record["id"] == "1234567"]
    assert ted["age"] == 27
    assert ted["favorite_color"] == "green"


def test_pseudonymize_records_fake(
    pvcy_client: PvcyClient, pii_records: List[Dict[str, Any]]
) -> None:
    treated_records = pvcy_client.pseudonymize_records(
        records=pii_records, suppresssion_method="fake", exclude_untreated=False
    )
    assert treated_records
    assert len(treated_records) == 2

    assert all(
        [
            sorted(list(record.keys())) == sorted(list(pii_records[0].keys()))
            for record in treated_records
        ]
    )
    [ted] = [record for record in treated_records if record["id"] == "1234567"]
    assert ted["first_name"] != "Ted"
    assert ted["email"] != "ted@example.com"
    assert "@" in ted["email"]
    assert ted["age"] == 27
    assert ted["favorite_color"] == "green"
