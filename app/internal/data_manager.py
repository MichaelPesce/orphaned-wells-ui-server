import logging
from pathlib import Path
import time

from typing import Optional, Dict, List
from pydantic import BaseModel

from app.internal.mongodb_connection import connectToDatabase
from app.internal.settings import AppSettings


_log = logging.getLogger(__name__)


class Project(BaseModel):
    """Information about a project."""

    # static information
    id_: str
    name: str
    description: str = ""
    state: str = ""
    history: List = []
    attributes: List = []
    documentType: str = ""


class DataManager:
    """Manage the active data."""

    VERSION = 1

    def __init__(self, **kwargs) -> None:
        self.app_settings = AppSettings(**kwargs)
        self.db = connectToDatabase()
        self.projects = []
        self.fetchProjects()

    def fetchProjects(self):
        cursor = self.db.projects.find({})
        for document in cursor:
            self.addProject(document)
        # _log.info(f"projects is : {self.projects}")

    def createProject(self, project_info):
        ## add project to db collection
        db_response = self.db.projects.insert_one(project_info)
        new_id = db_response.inserted_id

        ## add project to project list:
        cursor = self.db.projects.find({"_id": new_id})
        for document in cursor:
            self.addProject(document)

        return str(new_id)

    def addProject(self, document):
        p = Project(
            id_=str(document.get("_id", None)),
            name=document.get("name", ""),
            description=document.get("description", ""),
            state=document.get("state", ""),
            history=document.get("history", []),
            attributes=document.get("attributes", []),
            documentType=document.get("documentType", ""),
        )
        self.projects.append(p)

    def fetchProjectData(self, project_id):
        records = []
        cursor = self.db.records.find({"project_id": project_id})
        for document in cursor:
            # _log.info(f"found document: {document}")
            document["_id"] = str(document["_id"])
            records.append(document)
        return records


data_manager = DataManager()
