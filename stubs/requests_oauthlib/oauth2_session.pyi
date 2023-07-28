from typing import Any, Union

import requests
from oauthlib.oauth2 import BackendApplicationClient, OAuth2Token

class OAuth2Session(requests.Session):
    def __init__(
        self, client: Union[BackendApplicationClient, None] = ..., **kwargs: Any
    ) -> None: ...
    def fetch_token(
        self,
        token_url: str,
        client_id: Union[str, None] = ...,
        client_secret: Union[str, None] = ...,
        audience: Union[str, None] = ...,
        **kwargs: Any
    ) -> OAuth2Token: ...
