import logging
from pathlib import Path
import time

from typing import Optional, Dict, List
from pydantic import BaseModel

from app.internal.mongodb_connection import connectToDatabase


_log = logging.getLogger(__name__)


class Project(BaseModel):
    """Information about a project."""

    # static information
    id_: str
    name: str
    description: str = ""
    state: str = ""
    history: List = []


class DataManager:
    """Manage the active data."""

    VERSION = 1

    def __init__(self, **kwargs) -> None:
        self.img_path = Path.home() / ".uow" / "uploaded_images"
        self.img_path.mkdir(parents=True, exist_ok=True)
        self.db = connectToDatabase()
        self.projects = []
        self.fetchProjects()

    def fetchProjects(self):
        cursor = self.db.projects.find({})
        for document in cursor:
            self.addProject(document)
        _log.info(f"projects is : {self.projects}")

    def uploadProject(self, project_info):
        _log.info(f'uploading project with data: {project_info}')

    def addProject(self, document):
        p = Project(
            id_=str(document["_id"]),
            name=document["name"],
            description=document["description"],
            state=document["state"],
            history=document["history"],
        )
        self.projects.append(p)

    def fetchProjectData(self, project_id):
        records = []
        cursor = self.db.records.find({"project_id": project_id})
        _log.info(f'found cursor: {cursor}')
        for document in cursor:
            _log.info(f'found document: {document}')
            document["_id"] = str(document["_id"])
            records.append(document)
        return records


data_manager = DataManager()
