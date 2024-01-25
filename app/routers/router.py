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
    return data_manager.projects


@router.get("/get_project/{project_id}")
async def get_project_data(project_id: str):
    """
    Fetch project with provided project id
    Return project data
    """
    records = data_manager.fetchProjectData(project_id)
    return records
