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
    Depends,
)
from fastapi.responses import FileResponse
from fastapi.security import OAuth2PasswordBearer

# import copy

from app.internal.data_manager import data_manager, Project, Roles
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
    """Function authenticating API calls; required as a dependency for all API calls.

    Args:
        id_token: token provided upon signin

    Returns:
        user account information
    """
    try:
        user_info = id_token.verify_oauth2_token(
            token, google_requests.Request(), client_id
        )
        # _log.info(f"user_info: {user_info}")
        return user_info
    except Exception as e:  # should probably specify exception type
        ## return something to inform the frontend to prompt the user to log back in
        _log.info(f"unable to authenticate: {e}")
        raise HTTPException(status_code=401, detail=f"unable to authenticate: {e}")


@router.post("/auth_login")
async def auth_login(request: Request):
    """Function for logging into google account.

    Args:
        code: code provided by react google sign in

    Returns:
        user tokens (id_token, access_token, refresh_token)
    """
    code = await request.json()
    data = {
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": "postmessage",
        "grant_type": "authorization_code",
    }

    response = requests.post(token_uri, data=data)
    user_tokens = response.json()
    try:
        user_info = id_token.verify_oauth2_token(
            user_tokens["id_token"], google_requests.Request(), client_id
        )
    except Exception as e:  # should probably specify exception type
        _log.info(f"unable to authenticate: {e}")
        raise HTTPException(status_code=401, detail=f"unable to authenticate: {e}")
    role = data_manager.checkForUser(user_info)
    if role < Roles.base_user:
        _log.info(f"user is not authorized")
        raise HTTPException(status_code=403, detail=user_info)
    else:
        _log.info(f"user has role {role}")
        return user_tokens
    # else:
    #     _log.info(f"role not recognized: {role}")
    #     raise HTTPException(status_code=403, detail=user_info)


@router.post("/auth_refresh")
async def auth_refresh(request: Request):
    """Function for refreshing user tokens.

    Args:
        refresh_token: refresh token provided upon signin

    Returns:
        user tokens (id_token, access_token, refresh_token)
    """
    refresh_token = await request.json()
    data = {
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
    }
    response = requests.post(token_uri, data=data)
    user_tokens = response.json()
    try:
        user_info = id_token.verify_oauth2_token(
            user_tokens["id_token"], google_requests.Request(), client_id
        )
    except Exception as e:
        _log.info(f"unable to authenticate: {e}")
        raise HTTPException(status_code=401, detail=f"unable to authenticate: {e}")
    role = data_manager.checkForUser(user_info)
    if role < Roles.base_user:
        _log.info(f"user is not authorized")
        raise HTTPException(status_code=403, detail=user_info)
    else:
        _log.info(f"user has role {role}")
        return user_tokens


@router.post("/logout")
async def logout(request: Request):
    """Function for logging out and revoking tokens.

    Args:
        refresh_token: refresh token provided upon signin

    Returns:
        response code
    """
    refresh_token = await request.json()
    response = requests.post(
        "https://oauth2.googleapis.com/revoke",
        params={"token": refresh_token},
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    return {"logout_status": response.status_code}


@router.get("/get_projects", response_model=list)
async def get_projects(user_info: dict = Depends(authenticate)):
    """
    Fetch all projects
    """
    resp = data_manager.fetchProjects(user_info.get("email", ""))
    return resp


@router.get("/get_project/{project_id}")
async def get_project_data(project_id: str, user_info: dict = Depends(authenticate)):
    """Fetch project data.

    Args:
        project_id: Project identifier

    Returns:
        Project data, all records associated with that project
    """
    project_data, records = data_manager.fetchProjectData(
        project_id, user_info.get("email", "")
    )
    if project_data is None:
        raise HTTPException(
            403,
            detail=f"You do not have access to this project, please contact the project creator to gain access.",
        )
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


@router.post("/get_next_record")
async def get_next_record(request: Request, user_info: dict = Depends(authenticate)):
    """Fetch document record data.

    Args:
        record_id: Record identifier

    Returns:
        Record data
    """
    data = await request.json()
    record = data_manager.fetchNextRecord(
        data.get("dateCreated", ""), data.get("project_id", "")
    )
    return record


@router.post("/get_previous_record")
async def get_previous_record(
    request: Request, user_info: dict = Depends(authenticate)
):
    """Fetch document record data.

    Args:
        record_id: Record identifier

    Returns:
        Record data
    """
    data = await request.json()
    record = data_manager.fetchPreviousRecord(
        data.get("dateCreated", ""), data.get("project_id", "")
    )
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
    user_info: dict = Depends(authenticate),
):
    """Upload document for processing.Documents are processed asynchronously.

    Args:
        project_id: Project identifier to be associated with this document
        file: Document file

    Returns:
        New document record identifier.
    """
    original_output_path = f"{data_manager.app_settings.img_dir}/{file.filename}"
    filename, file_ext = os.path.splitext(file.filename)
    mime_type = file.content_type
    ## read document file
    try:
        async with aiofiles.open(original_output_path, "wb") as out_file:
            content = await file.read()  # async read
            await out_file.write(content)
        if file_ext == ".tif" or file_ext == ".tiff":
            _log.info(f"converting to png")
            output_path = convert_tiff(
                filename, file_ext, data_manager.app_settings.img_dir
            )
            file_ext = ".png"
            mime_type = "image/png"
        else:
            output_path = original_output_path
    except Exception as e:
        _log.error(f"unable to read image file: {e}")
        raise HTTPException(400, detail=f"Unable to process image file: {e}")

    ## add record to DB without attributes
    new_record = {
        "project_id": project_id,
        "name": filename,
        "filename": f"{filename}{file_ext}",
        "contributor": user_info,
        "status": "processing",
        "review_status": "unreviewed",
    }
    new_record_id = data_manager.createRecord(new_record)

    ## fetch processor id
    processor_id, processor_attributes = data_manager.getProcessor(project_id)

    ## upload to cloud storage (this will overwrite any existing files of the same name):
    background_tasks.add_task(
        upload_to_google_storage,
        file_path=output_path,
        file_name=f"{filename}{file_ext}",
        folder=f"uploads/{project_id}",
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
        processor_attributes=processor_attributes,
        data_manager=data_manager,
    )

    ## remove file after 120 seconds to allow for the operations to finish
    ## if file was converted to PNG, remove original file as well
    files_to_delete = [output_path]
    if original_output_path != output_path:
        files_to_delete.append(original_output_path)
    background_tasks.add_task(
        data_manager.deleteFiles, filepaths=files_to_delete, sleep_time=120
    )
    return {"record_id": new_record_id}


@router.post("/update_project/{project_id}")
async def update_project(
    project_id: str, request: Request, user_info: dict = Depends(authenticate)
):
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
async def update_record(
    record_id: str, request: Request, user_info: dict = Depends(authenticate)
):
    """Update record data.

    Args:
        record_id: Record identifier
        request data: New data for provided record

    Returns:
        Success response
    """
    req = await request.json()
    data = req.get("data", None)
    update_type = req.get("type", None)
    data_manager.updateRecord(record_id, data, update_type)

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


@router.post("/download_records/{project_id}", response_class=FileResponse)
async def download_records(
    project_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    user_info: dict = Depends(authenticate),
):
    """Download records for given project ID.

    Args:
        project_id: Project identifier

    Returns:
        CSV file containing all records associated with that project
    """
    req = await request.json()
    # _log.info(req)
    exportType = req.get("exportType", "csv")
    selectedColumns = req.get("columns", None)

    export_file = data_manager.downloadRecords(project_id, exportType, selectedColumns)
    ## remove file after 30 seconds to allow for the user download to finish
    background_tasks.add_task(
        data_manager.deleteFiles, filepaths=[export_file], sleep_time=30
    )
    return export_file


@router.post("/get_users/{role}")
async def get_users(
    role: str, request: Request, user_info: dict = Depends(authenticate)
):
    """Fetch all users from DB with role base_user or lower. Checks if user has proper role (admin)

    Returns:
        List of users, role types
    """
    req = await request.json()
    project_id = req.get("project_id", None)
    users = data_manager.getUsers(Roles[role], project_id_exclude=project_id)
    return users


@router.post("/add_contributors/{project_id}")
async def add_contributors(
    project_id: str, request: Request, user_info: dict = Depends(authenticate)
):
    """Add user to application database with role 'pending'

    Args:
        email: User email address

    Returns:
        user status
    """
    req = await request.json()
    users = req.get("users", "")
    return data_manager.addUsersToProject(users, project_id)


## admin functions
@router.post("/approve_user/{email}")
async def approve_user(email: str, user_info: dict = Depends(authenticate)):
    """Approve user for use of application by changing role from 'pending' to 'user'

    Args:
        email: User email address

    Returns:
        approved user information
    """
    if data_manager.hasRole(user_info, Roles.admin):
        return data_manager.approveUser(email)
    else:
        raise HTTPException(
            status_code=403, detail=f"User is not authorized to perform this operation"
        )


@router.post("/add_user/{email}")
async def add_user(email: str, user_info: dict = Depends(authenticate)):
    """Add user to application database with role 'pending'

    Args:
        email: User email address

    Returns:
        user status
    """
    if data_manager.hasRole(user_info, Roles.admin):
        ## TODO check if provided email is a valid email address

        ## this function will check for and then add user if it is not found
        role = data_manager.checkForUser({"email": email}, update=False)
        if role > 0:
            ## 406 Not acceptable: user provided an email that is already associated with an account
            raise HTTPException(status_code=406, detail=f"User is already created.")
        else:
            return {"pending": email}

    else:
        raise HTTPException(
            status_code=403, detail=f"User is not authorized to perform this operation"
        )


@router.post("/delete_user/{email}")
async def delete_user(email: str, user_info: dict = Depends(authenticate)):
    """Delete user from application database

    Args:
        email: User email address

    Returns:
        result
    """
    if data_manager.hasRole(user_info, Roles.admin):
        data_manager.deleteUser(email)
        return {"Deleted", email}

    else:
        raise HTTPException(
            status_code=403, detail=f"User is not authorized to perform this operation"
        )
