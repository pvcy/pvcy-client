from _typeshed import Incomplete
from requests.auth import AuthBase

class OAuth2(AuthBase):
    def __init__(
        self,
        client_id: Incomplete | None = ...,
        client: Incomplete | None = ...,
        token: Incomplete | None = ...,
    ) -> None: ...
    def __call__(self, r): ...
