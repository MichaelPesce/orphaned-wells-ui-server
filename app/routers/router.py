# import io
import os
from fastapi import (
    Body,
    Request,
    APIRouter,
    HTTPException,
    File,
    UploadFile,
    BackgroundTasks,
)
from fastapi.responses import StreamingResponse, FileResponse
import logging
import aiofiles
import asyncio

# import copy

from app.internal.data_manager import data_manager
from app.internal.image_handling import convert_tiff, upload_to_google_storage

_log = logging.getLogger(__name__)

router = APIRouter(
    prefix="",
    tags=["uow"],
    responses={404: {"description": "route not found"}},
)


@router.get("/get_projects")
async def get_projects():
    """
    Fetch all projects
    """
    return data_manager.fetchProjects()


@router.get("/get_project/{project_id}")
async def get_project_data(project_id: str):
    """
    Fetch project with provided project id
    Return project data
    """
    records = data_manager.fetchProjectData(project_id)
    project_data = next(
        (item for item in data_manager.projects if item.id_ == project_id), None
    )
    return {"project_data": project_data, "records": records}


@router.post("/add_project")
async def add_project(request: Request):
    """
    Fetch project with provided project id
    Return project data
    """
    data = await request.json()
    # _log.info(f"adding project with data: {data}")
    new_id = data_manager.createProject(data)
    return new_id


@router.post("/upload_document")
async def upload_document(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """
    Fetch project with provided project id
    Return project data
    """
    _log.info(f"uploading document: {file}")
    output_path = f'{data_manager.app_settings.img_dir}/{file.filename}'
    filename, file_ext = os.path.splitext(file.filename)
    ## read document file
    try:
        async with aiofiles.open(output_path, 'wb') as out_file:
            content = await file.read()  # async read
            await out_file.write(content)
        if file_ext == '.tif' or file_ext == '.tiff':
            _log.info(f"converting to png")
            output_path = convert_tiff(filename, file_ext, data_manager.app_settings.img_dir)
            file_ext = ".png"
    except Exception as e:
        _log.error(f'unable to read image file: {e}')
    _log.info(f"uploading document to: {output_path}")

    ## upload to cloud storage (this will overwrite any existing files of the same name):
    background_tasks.add_task(
        upload_to_google_storage,
        file_path=output_path,
        file_name=f"{filename}{file_ext}",
    )
    
    ## send to google doc AI 

    return {"response": "success"}
