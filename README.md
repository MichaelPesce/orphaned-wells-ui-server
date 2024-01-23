# orphaned-wells-ui-server
Backend server-side code for the orphaned wells UI

## Getting started (developer)

### Prerequisites

The following steps assume that:

1. `conda` is already installed and configured

### 1. Creating the Conda environment

Run the following command to create and activate a new Conda environment named `uow-server-env`:

```sh
conda env create --file environment.yml && conda activate uow-server-env
```

This will install the correct runtime versions of the backend (Python) and the backend dependencies.

### 2. Add credentials file

In order to use Google Cloud's document AI features, you must have access to a valid project and processor. The backend assumes that you will have the proper credentials stored in a python file called **creds.py**, located in **< orphaned-wells-ui-path >/backend/app/internal/**

**You must create that file and put it in that location, and the following variables must be stored in that file**:
    PROJECT_ID, LOCATION, PROCESSOR_ID

# Running the server

### Ensure that the `uow-server-env` Conda environment is active

```console
conda activate uow-ui-env
```

### Start server on port 8001

```console
cd <orphaned-wells-ui-path>/backend/app && uvicorn main:app --reload --host 127.0.0.1 --port 8001
```