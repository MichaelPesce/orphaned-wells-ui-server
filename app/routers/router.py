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
from app.internal.image_handling import (
    convert_tiff,
    upload_to_google_storage,
    process_image,
)

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
    """Upload document for processing.

    Args:
        project_id: Project identifier to be associated with this document
        file: Document file

    Returns:
        Success response. Documents are processed asynchronously
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
        data_manager=data_manager,
    )

    return {"request": "being processed"}


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
