# Contributing

This project is maintained by Privacy Dynamics. Issues are welcome from external contributors.

## Building this Project
This project is in Python and is built using Poetry.

1.  Install Python 3.9 or higher
2.  Install [Poetry](https://python-poetry.org/docs/) using any
    method; `pipx install poetry` is easiest if you already have pipx installed.
3.  Install the Poetry-dotenv [plugin](https://pypi.org/project/poetry-dotenv-plugin/)
    with `poetry self add poetry-dotenv-plugin`.
4.  Clone this project into a folder on your machine. `cd` to that folder.
5.  Create a new file, `.env`, in the root directory of this project. That file should
    contain the environment variables needed to run this project locally. It will
    look like this:
    ```sh
    PVCY_CLIENT_ID=<redacted>
    PVCY_CLIENT_SECRET=<redacted>
    PVCY_AUDIENCE=https://api.privacydynamics.io
    PVCY_BASE_URL=<redacted>
    PVCY_IDP_DOMAIN=https://auth.privacydynamics.io
    SNOWFLAKE_ACCOUNT=<redacted>
    SNOWFLAKE_PASSWORD=<redacted>
    ```
6.  Run `poetry install --sync` to install the dependencies for this project,
    including development and testing dependencies, into a virtual environment
    managed by Poetry. Ensure the Python version of the venv is at least 3.9
    (you should get a warning if that is not the case).
7.  Run `poetry shell` to spawn a subshell with the virtual environment activated
    and the environment variables loaded from the `.env` file.
9.  Run `make` to run unit tests and linters, or `make full` to run the full
    test suite, including integration tests with the API.
