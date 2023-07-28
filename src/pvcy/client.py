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
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Union
from uuid import UUID

from oauthlib.oauth2 import BackendApplicationClient, OAuth2Token
from requests import JSONDecodeError, Response, request
from requests_oauthlib import OAuth2Session

from pvcy.connection_types import (
    Connection,
    NewConnection,
    connection_factory,
)
from pvcy.exception import PvcyAuthError
from pvcy.project_types import (
    JobDefinition,
    NewJobDefinition,
    Project,
    TreatmentMethod,
)


@dataclass
class PvcyClient:
    """
    A client that wraps the Privacy Dynamics API.

    Args:
        base_url (str): The base URL for all API calls. Can be provided as the
            environment variable PVCY_BASE_URL instead of being passed into the
            class constructor. Defaults to https://api.privacydynamics.io.
        access_token (str | None): An OAuth2 Bearer Token for authenticating requests.
            If not provided, when the client is initialized, the token
            will be fetched using the client_id and client_secret from the
            idp_domain.
        client_id (str | None): The client_id for an OAuth2 client-credentials
            flow. Can be provided as the environment variable PVCY_CLIENT_ID
            instead of being passed into the class constructor.
        client_secret (str | None): The client_secret for an OAuth2 client-credentials
            flow. Can be provided as the environment variable PVCY_CLIENT_SECRET
            instead of being passed into the class constructor.
        audience (str | None): The identity claim's intended audience for an OAuth2
            client-credentials flow. Can be provided as the environment variable
            PVCY_AUDIENCE instead of being passed into the class constructor. Defaults
            to https://api.privacydynamics.io.
        idp_domain (str | None): The identity provider domain for an OAuth2
            client-credentials flow. Can be provided as the environment variable
            PVCY_IDP_DOMAIN instead of being passed into the class constructor.
            Defaults to https://auth.privacydynamics.io.
        raise_on_http_error (bool): Defaults to True; if False, errors are
            logged and responses are returned.
    """

    base_url: Union[str, None] = None
    access_token: Union[str, None] = field(default=None, repr=False)
    client_id: Union[str, None] = None
    client_secret: Union[str, None] = field(default=None, repr=False)
    audience: Union[str, None] = None
    idp_domain: Union[str, None] = None
    raise_on_http_error: bool = True

    def __post_init__(self) -> None:
        if self.base_url is None:
            self.base_url = (
                os.getenv("PVCY_BASE_URL") or "https://api.privacydynamics.io"
            )
        if self.access_token is None:
            try:
                self.client_id = self.client_id or os.environ["PVCY_CLIENT_ID"]
                self.client_secret = (
                    self.client_secret or os.environ["PVCY_CLIENT_SECRET"]
                )
                self.audience = (
                    self.audience
                    or os.environ["PVCY_AUDIENCE"]
                    or "https://api.privacydynamics.io"
                )
                self.idp_domain = (
                    self.idp_domain
                    or os.environ["PVCY_IDP_DOMAIN"]
                    or "https://auth.privacydynamics.io"
                )
            except KeyError as e:
                raise PvcyAuthError(
                    "You must provide a client_id and client_secret, "
                    "or have them set as environment variables."
                ) from e
            self.access_token = self._get_oauth_token()["access_token"]

    def _get_oauth_token(self, token_endpoint: str = "/oauth/token") -> OAuth2Token:
        """
        Returns an OAuth2 token fetched from the IDP domain using the
        client_id and client_secret.

        Args:
            token_endpoint (str): The endpoint to complete the token
                url, which will be constructed as f"{self.idp_domain}{token_endpoint}"

        Returns:
            OAuth2Token: A token object; the acces_token key contains the bearer
            token string.
        """

        client = BackendApplicationClient(client_id=self.client_id)
        oauth = OAuth2Session(client=client)

        logging.debug(f"Fetching token from {self.idp_domain}")

        token = oauth.fetch_token(
            token_url=f"{self.idp_domain}{token_endpoint}",
            client_id=self.client_id,
            client_secret=self.client_secret,
            audience=self.audience,
        )

        logging.debug(f"Token has scopes: {token['scope']}")
        return token

    def _handle_response(
        self, response: Response, method: str, endpoint: str
    ) -> Dict[str, Any]:
        try:
            json_data: Dict[str, Any] = response.json()
        except JSONDecodeError:
            logging.error(
                f"{method.upper()} {endpoint} responded with \n\n"
                f"{response.status_code}: {response.reason}"
            )
            return {}
        else:
            logging.debug(
                f"{method.upper()} {endpoint} responded with \n\n {json_data}"
            )
            if self.raise_on_http_error:
                response.raise_for_status()
            return json_data

    def _get(self, endpoint: str) -> Dict[str, Any]:
        """
        Executes a generic GET against any endpoint.

        Args:
            endpoint (str): should be everything after the base_url, e.g., /v1/projects

        Returns:
            Dict[str, Any]: The JSON-decoded response body.
        """
        logging.debug(f"Requesting GET to {endpoint}")
        response = request(
            "GET",
            f"{self.base_url}{endpoint}",
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
            },
        )
        data = self._handle_response(response, "GET", endpoint)
        return data

    def _post(
        self, endpoint: str, json_data: Any, params: Union[Dict[str, Any], None] = None
    ) -> Dict[str, Any]:
        """
        Executes a generic POST against any endpoint.

        Args:
            endpoint (str): should be everything after the base_url, e.g., /v1/projects
            json_data (Any): A python object (dict or list) that will be serialized
                as a json string by requests.

        Returns:
            Dict[str, Any]: The JSON-decoded response body.
        """
        logging.debug(f"Requesting POST to {endpoint} with data: {json_data}")
        response = request(
            "POST",
            f"{self.base_url}{endpoint}",
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Encoding": "deflate, br",
            },
            json=json_data,
            params=params,
        )
        data = self._handle_response(response, "POST", endpoint)
        return data

    def _put(self, endpoint: str, json_data: Any) -> Dict[str, Any]:
        """
        Executes a generic PUT against any endpoint.

        Args:
            endpoint (str): should be everything after the base_url, e.g., /v1/projects
            json_data (Any): A python object (dict or list) that will be serialized
                as a json string by requests.

        Returns:
            Dict[str, Any]: The JSON-decoded response body.
        """
        logging.debug(f"Requesting PUT to {endpoint} with data: {json_data}")
        response = request(
            "PUT",
            f"{self.base_url}{endpoint}",
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
            },
            json=json_data,
        )
        data = self._handle_response(response, "PUT", endpoint)
        return data

    def _delete(self, endpoint: str) -> None:
        """
        Executes a generic DELETE against any endpoint.

        Args:
            endpoint (str): should be everything after the base_url, e.g., /v1/projects

        Returns:
            None
        """
        logging.debug(f"Requesting DELETE to {endpoint}")
        response = request(
            "DELETE",
            f"{self.base_url}{endpoint}",
            headers={
                "Authorization": f"Bearer {self.access_token}",
            },
        )
        self._handle_response(response, "DELETE", endpoint)

    def get_connections(self) -> List[Connection]:
        """
        Return a list of all projects

        Returns:
            List[Connection]: All Connections accessible to the user.
        """
        response_data = self._get("/v1/connections")
        conns_json: List[Dict[str, Any]] = response_data.get("connections", [])
        return [connection_factory(c) for c in conns_json]

    def create_connection(
        self,
        connection: NewConnection,
    ) -> Connection:
        """
        Create a new connection in Privacy Dynamics

        Args:
            connection (NewConnection): An object containing
                new connection data.

        Returns:
            Connection: The new connection.
        """
        response_data = self._post(
            "/v1/connections",
            json_data=connection.model_dump(
                mode="json", by_alias=True, exclude_none=True
            ),
        )
        return connection_factory(response_data["connection"])

    def update_connection(
        self,
        connection_id: Union[str, UUID],
        connection: NewConnection,
    ) -> Connection:
        """
        Create a new connection in Privacy Dynamics

        Args:
            connection_id (str): The UUID4 for the connection to update.
            connection (NewConnection): An object containing new connection data.

        Returns:
            Connection: The connection after updating.
        """
        response_data = self._put(
            f"/v1/connections/{connection_id}",
            json_data=connection.model_dump(
                mode="json", by_alias=True, exclude_none=True
            ),
        )
        return connection_factory(response_data["connection"])

    def test_connection(self, connection_id: Union[str, UUID]) -> bool:
        """
        Test an existing connection, to verify that Privacy Dynamics can
        connect to the remote resource.

        Args:
            connection_id (str): The UUID4 for the connection to test.

        Returns:
            bool: True if the test succeeds, false otherwise.
        """
        response_data = self._post(
            f"/v1/connections/test/{connection_id}",
            json_data={},
        )
        return bool(response_data["can_connect"])

    def delete_connection(self, connection_id: Union[str, UUID]) -> None:
        """
        Delete an existing connection.

        Args:
            connection_id (str): The UUID4 for the connection to delete.

        Returns:
            None: If deletion succeeds, this method returns None.
        """
        self._delete(f"/v1/connections/{connection_id}")

    def get_projects(self) -> List[Project]:
        """
        Return a list of all projects.

        Returns:
            List[Project]: All Projects accessible to the user.
        """
        response_data = self._get("/v1/projects")
        projects_json = response_data.get("projects", [])
        return [Project(**p) for p in projects_json]

    def get_project(self, project_id: Union[str, UUID]) -> Project:
        """
        Return information about a single project

        Args:
            id (str): The UUID4 for the project.

        Returns:
            Project: The details of the requested Project.
        """
        response_data = self._get(f"/v1/projects/{project_id}")
        project_json = response_data.get("project", {})
        return Project(**project_json)

    def get_job_definitions(self, project_id: Union[str, UUID]) -> List[JobDefinition]:
        """
        Return a list of all job definitions

        Args:
            project_id (str): The UUID for the project whose job definitions are being
                requested.

        Returns:
            List[JobDefinition]: All JobDefinitions currently associated with the
            Project.
        """
        response_data = self._get(f"/v1/projects/{project_id}/job_definitions")
        jd_json = response_data.get("job_definitions", [])
        return [JobDefinition(**job) for job in jd_json]

    def get_job_definition(
        self, project_id: Union[str, UUID], job_definition_id: Union[str, UUID]
    ) -> JobDefinition:
        """
        Return information for a single job definition.

        Args:
            project_id (str): The UUID for the project whose job definitions are being
                requested.
            job_definition_id (str): The UUID for the job definition whose info is
                being requested.

        Returns:
            JobDefinition: A JobDefinition currently associated with the Project.
        """
        response_data = self._get(
            f"/v1/projects/{project_id}/job_definitions/{job_definition_id}"
        )
        jd_json = response_data.get("job_definition", {})
        return JobDefinition(**jd_json)

    def create_job_definition(
        self, project_id: Union[str, UUID], job_definition: NewJobDefinition
    ) -> JobDefinition:
        """
        Create a new Job Definition from the spec in job_definition

        Args:
            project_id (str): The UUID for the project that will contain the
                new job definition.
            job_definition (NewJobDefinition): An object containing the new
                job definition data.

        Returns:
            JobDefinition: The JobDefinition that was created.
        """
        response_data = self._post(
            f"/v1/projects/{project_id}/job_definitions",
            json_data=job_definition.model_dump(
                mode="json", by_alias=True, exclude_none=True
            ),
        )
        return JobDefinition(**response_data["job_definition"])

    def update_job_definition(
        self,
        project_id: Union[str, UUID],
        job_definition_id: Union[str, UUID],
        job_definition: NewJobDefinition,
    ) -> JobDefinition:
        """
        Replace a Job Definition with the new values provided in job_definition.

        Args:
            project_id (str): The UUID for the project that will contain the
                new job definition.
            job_definition (NewJobDefinition): An object containing the new
                job definition data.

        Returns:
            JobDefinition: The JobDefinition that was updated.
        """
        response_data = self._put(
            f"/v1/projects/{project_id}/job_definitions/{job_definition_id}",
            json_data=job_definition.model_dump(
                mode="json", by_alias=True, exclude_none=True
            ),
        )
        return JobDefinition(**response_data["job_definition"])

    def create_or_update_job_definitions(
        self, project_id: Union[str, UUID], job_definitions: List[NewJobDefinition]
    ) -> List[JobDefinition]:
        """
        For each spec in job_definitions, create a new Job Definition or replace
        an existing one. Job Definitions will only be replaced if the
        NewJobDefinition contains the UUID for the existing Job Definition.

        Args:
            project_id (str): The UUID for the project that will contain the
                new or updated job definitions.
            job_definitions (List[NewJobDefinition]): A list of objects containing
                the new job definition data.

        Returns:
            JobDefinition: The JobDefinition that was created.
        """
        # The endpoint requires separate calls for creates and updates, so we split
        # the job_definitions list into two batches, and operate on each batch.
        create_jds = [jd for jd in job_definitions if jd.job_definition_id is None]
        update_jds = [jd for jd in job_definitions if jd.job_definition_id is not None]
        ret_jds: List[JobDefinition] = []
        if create_jds:
            response_data = self._post(
                f"/v1/projects/{project_id}/job_definitions:batch_create",
                json_data={
                    "job_definitions": [
                        jd.model_dump(mode="json", by_alias=True, exclude_none=True)
                        for jd in create_jds
                    ]
                },
            )
            ret_jds.extend(
                [
                    JobDefinition(**jd_dict)
                    for jd_dict in response_data["job_definitions"]
                ]
            )
        if update_jds:
            for jd in update_jds:
                assert jd.job_definition_id is not None
                ret_jds.append(
                    self.update_job_definition(
                        project_id=project_id,
                        job_definition_id=jd.job_definition_id,
                        job_definition=jd,
                    )
                )
        return ret_jds

    def delete_job_definition(
        self, project_id: Union[str, UUID], job_definition_id: Union[str, UUID]
    ) -> None:
        """
        Return information for a single job definition.

        Args:
            project_id (str): The UUID for the project whose job definition is being
                deleted.
            job_definition_id (str): The UUID for the job definition that should
                be deleted.
        """
        self._delete(f"/v1/projects/{project_id}/job_definitions/{job_definition_id}")

    def pseudonymize_records(
        self,
        records: List[Dict[str, Any]],
        suppresssion_method: Union[
            Literal["hide", "mask_full", "mask", "fake", "none"], TreatmentMethod
        ] = "hide",
        exclude_untreated: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Return a treated version of `records`, with direct identifiers
        suppressed using the specified method.

        Args:
            records (List[Dict[str, Any]]): One or more "rows" of data,
                where the dict's keys are the column headers, and the
                values are the cell values.
            suppression_method (str | TreatmentMethod): One of "hide" (redact
                values by dropping them from the returned document),
                "fake" (replace values with realistic-looking fakes of the
                same semantic type), "mask" (replace values with "#"), or
                "none" (do not treat). Defaults to "hide".
            exclude_untreated (bool): If True, the response does not include
                values that were not changed by the pseudonymizer. Defaults
                to False (returns treated and untreated keys for each record).

        Returns
            List[Dict[str, Any]]: The treated list of records.
        """
        data = {"table_records": records}
        response_data = self._post(
            "/preview/table_pseudonymizer",
            json_data=data,
            params={
                "did_suppression_method": suppresssion_method,
                "exclude_untreated": exclude_untreated,
            },
        )
        return response_data.get("table_records", [])
