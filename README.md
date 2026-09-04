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

This will install the correct runtime versions of the backend (Python) and the backend dependencies.\
\
Alternatively, if you already have an environment that you would like to install the dependencies in, 
activate your environment and run the command:
```sh
pip install .
```

#### For Developers:

This section is for developers who plan to modify or contribute to the server's codebase. In the same environment
where you installed the package, run the following command:
```sh
pip install -r requirements-dev.txt
```

### 2. Add credential/environment files

Credentials are necessary for backend functionality. This includes Google Cloud's Document AI features, MongoDB database access, and Google Cloud Storage for storing documents/images. To access these functionalities, create the following files and place local service-account JSON keys in the **< orphaned-wells-ui-server-path >/ogrre/** directory or use absolute paths in `.env`:
1. **.env** 
    - Must contain runtime values such as **PROJECT_ID**, **LOCATION**, **DB_USERNAME**, **DB_PASSWORD**, **DB_CONNECTION**, **DB_NAME**, **STORAGE_BUCKET_NAME**, **STORAGE_SERVICE_KEY**, and **DOCUMENT_AI_SERVICE_KEY** when using Google backends.
2. **STORAGE_SERVICE_KEY** file
    - Service-account JSON key used only for Google Cloud Storage.
    - The filename/path must match `STORAGE_SERVICE_KEY` in `.env`.
3. **DOCUMENT_AI_SERVICE_KEY** file
    - Service-account JSON key used only for Google Document AI processing and processor deployment.
    - The filename/path must match `DOCUMENT_AI_SERVICE_KEY` in `.env`.

# Running the server

### Ensure that the `uow-server-env` Conda environment is active

```console
conda activate uow-server-env
```

### Start server on port 8001

```console
cd <orphaned-wells-ui-server-path>/app && uvicorn main:app --reload --host 127.0.0.1 --port 8001
```
