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


from datetime import datetime
from enum import Enum
from typing import Any, Dict, Type, Union

from pydantic import (
    UUID4,
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class ConnectionType(Enum):
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    POSTGRES = "postgres"
    REDSHIFT = "redshift"
    MYSQL = "mysql"
    S3 = "s3"
    S3_SAMPLE = "s3_sample"
    SFTP = "sftp"


class PvcyBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    @field_validator("*", mode="before")
    def str_none_is_none(cls, v: str) -> Union[str, None]:
        if v == "None":
            return None
        else:
            return v


class _NamedConnection(PvcyBaseModel):
    connection_name: str
    input_type: ConnectionType = Field(
        validation_alias=AliasChoices("input_type", "connection_input_type")
    )


class SimpleConnection(_NamedConnection):
    connection_id: UUID4
    database: Union[str, None]


class _ExistingConnectionMixIn(PvcyBaseModel):
    connection_id: UUID4
    created_at: datetime = Field(alias="created")
    updated_at: datetime = Field(alias="updated")


class _OptionalPasswordMixin(PvcyBaseModel):
    password: Union[str, None] = Field(None, repr=False)


class _PasswordMixin(PvcyBaseModel):
    password: str = Field(..., repr=False)


class _OptionalPrivateKeyMixin(PvcyBaseModel):
    private_key: Union[str, None] = Field(None, repr=False)


class _PrivateKeyMixin(PvcyBaseModel):
    private_key: str = Field(..., repr=False)


class _HostPortMixin(PvcyBaseModel):
    host: str
    port: int


class SshConnection(_HostPortMixin, _OptionalPrivateKeyMixin):
    username: str


class _SshMixin(PvcyBaseModel):
    ssh_tunnel: Union[SshConnection, None] = None


class NewSshConnection(_HostPortMixin, _PrivateKeyMixin):
    username: str


class _NewSshMixin(PvcyBaseModel):
    ssh_tunnel: Union[NewSshConnection, None] = None


class _SnowflakeBase(_NamedConnection):
    input_type: ConnectionType = ConnectionType.SNOWFLAKE
    account: str
    username: str = Field(
        validation_alias=AliasChoices("user", "username"), serialization_alias="user"
    )
    database: str
    db_schema: Union[str, None] = Field(default=None, alias="schema")
    warehouse: Union[str, None] = None
    role: Union[str, None] = None


class SnowflakeConnection(
    _SnowflakeBase, _ExistingConnectionMixIn, _OptionalPasswordMixin
):
    pass


class NewSnowflakeConnection(_SnowflakeBase, _PasswordMixin):
    pass


class _BigqueryBase(_NamedConnection):
    input_type: ConnectionType = ConnectionType.BIGQUERY
    bigquery_project_id: str
    dataset: Union[str, None] = None


class BigqueryConnection(_BigqueryBase, _ExistingConnectionMixIn):
    pass


class NewBigqueryConnection(_BigqueryBase):
    keyfile_json: str


class _PostgresBase(_NamedConnection, _HostPortMixin):
    input_type: ConnectionType = ConnectionType.POSTGRES
    database: str
    username: str


class PostgresConnection(
    _PostgresBase, _ExistingConnectionMixIn, _OptionalPasswordMixin, _SshMixin
):
    pass


class NewPostgresConnection(_PostgresBase, _PasswordMixin, _NewSshMixin):
    pass


class _RedshiftBase(_PostgresBase):
    input_type: ConnectionType = ConnectionType.REDSHIFT


class RedshiftConnection(
    _RedshiftBase, _ExistingConnectionMixIn, _OptionalPasswordMixin, _SshMixin
):
    pass


class NewRedshiftConnection(_RedshiftBase, _PasswordMixin, _NewSshMixin):
    pass


class _MysqlBase(_NamedConnection, _HostPortMixin):
    input_type: ConnectionType = ConnectionType.MYSQL
    username: str


class MysqlConnection(
    _MysqlBase, _ExistingConnectionMixIn, _OptionalPasswordMixin, _SshMixin
):
    pass


class NewMysqlConnection(_MysqlBase, _PasswordMixin, _NewSshMixin):
    pass


class _S3Base(_NamedConnection):
    input_type: ConnectionType = ConnectionType.S3
    region_name: str
    aws_access_key_id: str
    endpoint_url: Union[str, None] = None


class S3Connection(_S3Base, _ExistingConnectionMixIn):
    pass


class S3SampleConnection(_NamedConnection, _ExistingConnectionMixIn):
    input_type: ConnectionType = ConnectionType.S3_SAMPLE


class NewS3Connection(_S3Base):
    aws_secret_access_key: str = Field(repr=False)


class _SftpBase(_NamedConnection, _HostPortMixin):
    input_type: ConnectionType = ConnectionType.SFTP
    username: str


class SftpConnection(_SftpBase, _ExistingConnectionMixIn, _OptionalPasswordMixin):
    pass


class NewSftpConnection(_SftpBase, _OptionalPasswordMixin, _OptionalPrivateKeyMixin):
    @model_validator(mode="after")
    def check_password_or_pk(self) -> "NewSftpConnection":
        if self.password is None and self.private_key is None:
            raise ValueError("Must provide either a password or a private key.")
        elif self.password is not None and self.private_key is not None:
            raise ValueError("Cannot provide both a password and a private key.")
        return self


Connection = Union[
    SnowflakeConnection,
    BigqueryConnection,
    PostgresConnection,
    RedshiftConnection,
    MysqlConnection,
    S3Connection,
    S3SampleConnection,
    SftpConnection,
]


def connection_factory(connection_data: Dict[str, Any]) -> Connection:
    """
    Creates a Connection from its dict representation.
    """
    raw = connection_data.get("input_type")
    assert raw is not None, "Error! Connection has no input_type key."
    input_type: ConnectionType = ConnectionType(raw)
    mapping: Dict[ConnectionType, Type[Connection]] = {
        ConnectionType.SNOWFLAKE: SnowflakeConnection,
        ConnectionType.BIGQUERY: BigqueryConnection,
        ConnectionType.POSTGRES: PostgresConnection,
        ConnectionType.REDSHIFT: RedshiftConnection,
        ConnectionType.MYSQL: MysqlConnection,
        ConnectionType.S3: S3Connection,
        ConnectionType.S3_SAMPLE: S3SampleConnection,
        ConnectionType.SFTP: SftpConnection,
    }
    assert (
        input_type in mapping
    ), f"Error! Could not initialize Connection class for input_type {raw}"
    return mapping[input_type](**connection_data)


NewConnection = Union[
    NewSnowflakeConnection,
    NewBigqueryConnection,
    NewPostgresConnection,
    NewRedshiftConnection,
    NewMysqlConnection,
    NewS3Connection,
    NewSftpConnection,
]
