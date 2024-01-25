# import io
# import os
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

# import aiofiles
# import copy

from app.internal.data_manager import data_manager

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
async def upload_document(file: UploadFile = File(...)):
    """
    Fetch project with provided project id
    Return project data
    """
    _log.info(f"uploading document: {file}")
    return {"response": "success"}
