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

from typing import Any, List

import pytest
from oauthlib.oauth2 import OAuth2Token
from pvcy.client import PvcyClient
from pvcy.exception import PvcyAuthError
from requests_oauthlib import OAuth2Session

from .utils import load_response_from_file


@pytest.fixture
def mock_pvcy_oauth_token(monkeypatch: pytest.MonkeyPatch) -> None:
    class MockOAuth2Token(OAuth2Token):
        scope: List[str] = list()

        def __init__(
            self,
            access_token: str,
            scope: List[str],
            expires_in: int,
            token_type: str,
            expires_at: float,
        ):
            self.access_token = access_token
            self.scope = scope
            self.expires_in = expires_in
            self.token_type = token_type
            self.expires_at = expires_at

        def __getitem__(self, k: Any) -> Any:
            return getattr(self, k)

    class MockOAuth2Session(OAuth2Session):
        def fetch_token(self, *args: Any, **kwargs: Any) -> OAuth2Token:
            return MockOAuth2Token(**load_response_from_file("auth/fetch_token.json"))

    monkeypatch.setattr("pvcy.client.OAuth2Session", MockOAuth2Session)


@pytest.fixture
def pvcy_env_vars() -> List[str]:
    return [
        "PVCY_CLIENT_ID",
        "PVCY_CLIENT_SECRET",
        "PVCY_IDP_DOMAIN",
    ]


def test_init(
    monkeypatch: pytest.MonkeyPatch,
    pvcy_env_vars: List[str],
    mock_pvcy_oauth_token: None,
) -> None:
    for env in pvcy_env_vars:
        monkeypatch.setenv(env, "foo")

    client = PvcyClient()
    assert client.access_token is not None
    assert client.access_token.startswith("eyJo")


def test_raises_missing_auth_info(
    monkeypatch: pytest.MonkeyPatch, pvcy_env_vars: List[str]
) -> None:
    for env in pvcy_env_vars:
        monkeypatch.delenv(env, raising=False)

    with pytest.raises(PvcyAuthError):
        _ = PvcyClient()
