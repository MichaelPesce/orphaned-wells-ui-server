import io
import os
from fastapi import Body, Request, APIRouter, HTTPException, File, UploadFile, BackgroundTasks
from fastapi.responses import StreamingResponse, FileResponse
import logging
import aiofiles
import copy

from app.internal.ocr_stuff import get_tokens_inside_coordinates, parse_image, convert_tiff, get_tokens_inside_normalized_coordinates
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
    Fetch formfields for a given image
    """
    return data_manager.projects