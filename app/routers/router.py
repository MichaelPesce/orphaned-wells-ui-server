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
        # _log.info(f"user_info: {user_info}")
        return user_info
    except Exception as e: # should probably specify exception type
        ## return something to inform the frontend to prompt the user to log back in
        _log.info(f"unable to authenticate: {e}")
        raise HTTPException(status_code=401, detail=f"unable to authenticate: {e}")


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
    data = {
        'code': code,
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': 'postmessage',
        'grant_type': 'authorization_code'
    }

    response = requests.post(token_uri, data=data)
    user_tokens = response.json()
    # access_token = user_tokens["access_token"]
    # refresh_token = user_tokens["refresh_token"]
    # _log.info(f"access token: {access_token}\nrefresh token: {refresh_token}\nid token: {user_tokens['id_token']}")
    # id_token = user_tokens["id_token"]
    try:
        user_info = id_token.verify_oauth2_token(user_tokens["id_token"], google_requests.Request(), client_id)
    except Exception as e: # should probably specify exception type
        ## return something to inform the frontend to prompt the user to log back in
        _log.info(f"unable to authenticate: {e}")
        raise HTTPException(status_code=401, detail=f"unable to authenticate: {e}")
    role = data_manager.checkForUser(user_info)
    if role is None or role == "pending":
        _log.info(f"user is not authorized")
        raise HTTPException(status_code=403, detail=user_info)
    else:
        _log.info(f"user has role {role}")
        return user_tokens


@router.post("/auth_refresh")
async def auth_refresh(request: Request):
    _log.info("attempting to refresh tokens")
    refresh_token = await request.json()
    data = {
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'refresh_token'
    }
    response = requests.post(token_uri, data=data)
    user_tokens = response.json()
    try:
        user_info = id_token.verify_oauth2_token(user_tokens["id_token"], google_requests.Request(), client_id)
    except Exception as e:
        _log.info(f"unable to authenticate: {e}")
        raise HTTPException(status_code=401, detail=f"unable to authenticate: {e}")
    role = data_manager.checkForUser(user_info)
    if role is None or role == "pending":
        _log.info(f"user is not authorized")
        raise HTTPException(status_code=403, detail=user_info)
    else:
        _log.info(f"user has role {role}")
        return user_tokens


@router.get("/get_projects", response_model=list)
async def get_projects(user_info: dict = Depends(authenticate)):
    """
    Fetch all projects
    """
    resp = data_manager.fetchProjects(user_info.get("email",""))
    return resp


@router.get("/get_project/{project_id}")
async def get_project_data(project_id: str, user_info: dict = Depends(authenticate)):
    """Fetch project data.

    Args:
        project_id: Project identifier

    Returns:
        Project data, all records associated with that project
    """
    project_data, records = data_manager.fetchProjectData(project_id, user_info.get("email",""))
    if project_data is None:
        raise HTTPException(403, detail=f"You do not have access to this project, please contact the project creator to gain access.")
    return {"project_data": project_data, "records": records}


@router.get("/get_record/{record_id}")
async def get_record_data(record_id: str, user_info: dict = Depends(authenticate)):
    """Fetch document record data.

    Args:
        record_id: Record identifier

    Returns:
        Record data
    """
    record = data_manager.fetchRecordData(record_id)
    return record


@router.post("/add_project")
async def add_project(request: Request, user_info: dict = Depends(authenticate)):
    """Add new project.

    Args:
        request data: Project data

    Returns:
        New project identifier
    """
    data = await request.json()
    
    # _log.info(f"adding project with data: {data}")
    new_id = data_manager.createProject(data, user_info)
    return new_id


@router.post("/upload_document/{project_id}")
async def upload_document(
    project_id: str,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    user_info: dict = Depends(authenticate)
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
        "contributor": user_info
    }
    new_record_id = data_manager.createRecord(new_record)

    ## fetch processor id
    processor_id = data_manager.getProcessor(project_id)

    ## upload to cloud storage (this will overwrite any existing files of the same name):
    background_tasks.add_task(
        upload_to_google_storage,
        file_path=output_path,
        file_name=f"{filename}{file_ext}",
        folder=f"uploads/{project_id}"
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
async def update_project(project_id: str, request: Request, user_info: dict = Depends(authenticate)):
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
async def update_record(record_id: str, request: Request, user_info: dict = Depends(authenticate)):
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
async def delete_project(project_id: str, user_info: dict = Depends(authenticate)):
    """Delete project.

    Args:
        project_id: Project identifier

    Returns:
        Success response
    """
    data_manager.deleteProject(project_id)

    return {"response": "success"}


@router.post("/delete_record/{record_id}")
async def delete_record(record_id: str, user_info: dict = Depends(authenticate)):
    """Delete record.

    Args:
        record_id: Record identifier

    Returns:
        Success response
    """
    data_manager.deleteRecord(record_id)

    return {"response": "success"}


@router.get("/download_records/{project_id}", response_class=FileResponse)
async def download_records(project_id: str, user_info: dict = Depends(authenticate)):
    """Download records for given project ID.

    Args:
        project_id: Project identifier

    Returns:
        CSV file containing all records associated with that project
    """
    csv_output = data_manager.downloadRecords(project_id)

    return csv_output

