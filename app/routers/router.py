# import io
import os
import logging
import aiofiles
import requests
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from typing import Annotated
import jwt
from fastapi import (
    # Body,
    Request,
    APIRouter,
    HTTPException,
    File,
    UploadFile,
    BackgroundTasks,
    Depends
)
from fastapi.responses import FileResponse
from fastapi.security import OAuth2PasswordBearer

# import copy

from app.internal.data_manager import data_manager, Project
from app.internal.image_handling import (
    convert_tiff,
    upload_to_google_storage,
    process_image,
)
import app.internal.auth as auth

_log = logging.getLogger(__name__)

token_uri, client_id, client_secret = auth.get_google_credentials()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

router = APIRouter(
    prefix="",
    tags=["uow"],
    responses={404: {"description": "route not found"}},
)


@router.post("/token")
async def authenticate(token: str = Depends(oauth2_scheme)):
    try:
        user_info = id_token.verify_oauth2_token(token, google_requests.Request(), client_id)
        return user_info
    except Exception as e: # should probably specify exception type
        _log.info(f"unable to authenticate: {e}")
        raise HTTPException(status_code=401, detail=f"unable to authenticate: {e}")
        ## return something to inform the frontend to prompt the user to log back in

    return "user info"


@router.post("/auth_login")
async def auth_login(request: Request):
    """Update record data.

    Args:
        record_id: Record identifier
        request data: New data for provided record

    Returns:
        Success response
    """
    code = await request.json()
    # token_uri, client_id, client_secret = auth.get_google_credentials()

    data = {
        'code': code,
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': 'postmessage',
        'grant_type': 'authorization_code'
    }

    response = requests.post(token_uri, data=data)
    # _log.info(f"response json: {response.json()}")
    return response.json()


@router.post("/auth_refresh")
async def auth_refresh(request: Request):
    refresh_token = await request.json()
    # token_uri, client_id, client_secret = auth.get_google_credentials()
    data = {
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'refresh_token'
    }

    response = requests.post(token_uri, data=data)

    return response.json()

@router.get("/get_projects", response_model=list)
async def get_projects(user_info: dict = Depends(authenticate)):
    """
    Fetch all projects
    """
    resp = data_manager.fetchProjects()
    return resp


@router.get("/get_project/{project_id}")
async def get_project_data(project_id: str):
    """Fetch project data.

    Args:
        project_id: Project identifier

    Returns:
        Project data, all records associated with that project
    """
    records = data_manager.fetchProjectData(project_id)
    project_data = next(
        (item for item in data_manager.projects if item.id_ == project_id), None
    )
    return {"project_data": project_data, "records": records}


@router.get("/get_record/{record_id}")
async def get_record_data(record_id: str):
    """Fetch document record data.

    Args:
        record_id: Record identifier

    Returns:
        Record data
    """
    record = data_manager.fetchRecordData(record_id)
    return record


@router.post("/add_project")
async def add_project(request: Request):
    """Add new project.

    Args:
        request data: Project data

    Returns:
        New project identifier
    """
    data = await request.json()
    # _log.info(f"adding project with data: {data}")
    new_id = data_manager.createProject(data)
    return new_id


@router.post("/upload_document/{project_id}")
async def upload_document(
    project_id: str, background_tasks: BackgroundTasks, file: UploadFile = File(...)
):
    """Upload document for processing.Documents are processed asynchronously.

    Args:
        project_id: Project identifier to be associated with this document
        file: Document file

    Returns:
        New document record identifier.
    """
    output_path = f"{data_manager.app_settings.img_dir}/{file.filename}"
    filename, file_ext = os.path.splitext(file.filename)
    mime_type = file.content_type
    ## read document file
    try:
        async with aiofiles.open(output_path, "wb") as out_file:
            content = await file.read()  # async read
            await out_file.write(content)
        if file_ext == ".tif" or file_ext == ".tiff":
            _log.info(f"converting to png")
            output_path = convert_tiff(
                filename, file_ext, data_manager.app_settings.img_dir
            )
            file_ext = ".png"
            mime_type = "image/png"
    except Exception as e:
        _log.error(f"unable to read image file: {e}")
        raise HTTPException(400, detail=f"Unable to process image file: {e}")

    ## add record to DB without attributes
    new_record = {
        "project_id": project_id,
        "filename": f"{filename}{file_ext}",
    }
    new_record_id = data_manager.createRecord(new_record)

    ## fetch processor id
    processor_id = data_manager.getProcessor(project_id)

    ## upload to cloud storage (this will overwrite any existing files of the same name):
    background_tasks.add_task(
        upload_to_google_storage,
        file_path=output_path,
        file_name=f"{filename}{file_ext}",
    )

    ## send to google doc AI
    background_tasks.add_task(
        process_image,
        file_path=output_path,
        file_name=f"{filename}{file_ext}",
        mime_type=mime_type,
        project_id=project_id,
        record_id=new_record_id,
        processor_id=processor_id,
        data_manager=data_manager,
    )

    return {"record_id": new_record_id}


@router.post("/update_project/{project_id}")
async def update_project(project_id: str, request: Request):
    """Update project data.

    Args:
        project_id: Project identifier
        request data: New data for provided project

    Returns:
        Success response
    """
    data = await request.json()
    data_manager.updateProject(project_id, data)

    return {"response": "success"}


@router.post("/update_record/{record_id}")
async def update_record(record_id: str, request: Request):
    """Update record data.

    Args:
        record_id: Record identifier
        request data: New data for provided record

    Returns:
        Success response
    """
    data = await request.json()
    data_manager.updateRecord(record_id, data)

    return {"response": "success"}


@router.post("/delete_project/{project_id}")
async def update_pdelete_projectroject(project_id: str):
    """Delete project.

    Args:
        project_id: Project identifier

    Returns:
        Success response
    """
    data_manager.deleteProject(project_id)

    return {"response": "success"}


@router.post("/delete_record/{record_id}")
async def delete_record(record_id: str):
    """Delete record.

    Args:
        record_id: Record identifier

    Returns:
        Success response
    """
    data_manager.deleteRecord(record_id)

    return {"response": "success"}


@router.get("/download_records/{project_id}", response_class=FileResponse)
async def download_records(project_id: str):
    """Download records for given project ID.

    Args:
        project_id: Project identifier

    Returns:
        CSV file containing all records associated with that project
    """
    csv_output = data_manager.downloadRecords(project_id)

    return csv_output

