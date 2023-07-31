# pvcy-client: A Python Client for the Privacy Dynamics API

This project provides a Python Client for the Privacy Dynamics API. To
use this client, you will need a Privacy Dynamics account with API access
enabled.

## Installation

```sh
pip install git+https://github.com/pvcy/pvcy-client.git@main
```

## Getting Started

1.  Log into your Privacy Dynamics account; if you are using the SaaS version,
    you will log in at `https://app.privacydynamics.io/`
2.  On the top nav, select "Settings", then on the left nav, select "Users".
3.  Above the table that lists users, there is a button labeled "API Access".
    Select that button to view your API credentials.
4.  You will need to pass the values from this screen into the Privacy Dynamics
    client when it is initialized. One of the easiest ways to do this is to
    create a `.env` file with the following keys:
    ```sh
    PVCY_BASE_URL=<your base URL> # default: https://api.privacydynamics.io
    PVCY_CLIENT_ID=<your Client ID>
    PVCY_CLIENT_SECRET=<your Client Secret>
    PVCY_AUDIENCE=<your Audience or Base URL> # default: https://api.privacydynamics.io
    PVCY_IDP_DOMAIN=<your Identity Provider> # default: https://auth.privacydynamics.io
    ```
    You will need to load the values from the `.env` file before initializing
    the `PvcyClient` in the next step.

### Using the API
1.  Import and initialize a client. This will automatically use your credentials
    to fetch an OAuth2 token from the identity provider.
    ```py
    from pvcy import PvcyClient

    # No arguments required if environment variables are set.
    client = PvcyClient()
    ```
2.  Use the client to interact with the API. For example, list projects:
    ```py
    projects = client.get_projects()
    ```

### Using the CLI

To export the configuration from the API and save it to a YML config file:

```bash
pvcy export
```

To merge new config with the state from the API:

```bash
pvcy sync
```

To merge only specific projects:

```bash
pvcy sync --only 87d3f199-4f74-4062-88ad-3c75cb406501
```

To copy Job Definition config from one project to one or more other projects:

```bash
pvcy copy 87d3f199-4f74-4062-88ad-3c75cb406501 48ce4248-4a69-403a-adab-9e8fa3464d31 814717f4-1a86-40c4-9dc4-ec53806553bc
```

(The first argument is the ID of the source project; the other arguments are the destination
project(s)).

To view all options and get help:

```bash
pvcy --help
pvcy export --help
pvcy sync --help
pvcy copy --help
```

## Additional Documentation

To view the documentation for the client, use the built-in `help`:

```py
from pvcy import PvcyClient
help(PvcyClient)
```

### Types

The client uses Pydantic models to validate the schema for the API requests and responses.

Request schemas are prefixed with `New`; response schemas are not. For example, you can
create a new connection by passing a `NewSnowflakeConnection` to `PvcyClient.create_connection`.
That method then returns a `SnowflakeConnection`.

A full list of Connection types is as follows:

```py
from pvcy import (
    # Connection Schemas (New Connection Requests)
    NewConnection, # Union type for any connection schema
    NewBigqueryConnection,
    NewMysqlConnection,
    NewPostgresConnection,
    NewRedshiftConnection,
    NewS3Connection,
    NewSftpConnection,
    NewSnowflakeConnection,
    NewSshConnection, # Nested type for conns that support SSH

    # Connections
    Connection, # Union type for any connection
    ConnectionType, # Enum of connection types (e.g., snowflake, mysql)
    connection_factory, # Function to build a Connection from an API response
    BigqueryConnection,
    MysqlConnection,
    PostgresConnection,
    RedshiftConnection,
    S3Connection,
    S3SampleConnection,
    SftpConnection,
    SnowflakeConnection,
    SshConnection,
)
```

Additionally, project config is typed. A full list of project types:

```py
from pvcy import (
    Project, # response type from /projects endpoints
    ConfigFile, # Schema for YAML config file 
    ProjectConfig, # Schema for project definition in YAML config file

    JobDefinition, # Response type
    NewJobDefinition, # Request schema for API and YAML config

    NameSpace,
    JobRunType, # Enum for JobDefinition property
    KStrategy, # Enum for JobDefinition property
    ColumnConfig,
    PiiClass, # Enum for ColumnConfig property
    TreatmentMethod, # Enum for ColumnConfig property
)
```