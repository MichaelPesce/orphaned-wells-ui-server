import logging
from pathlib import Path
import time

from typing import Optional, Dict, List
from pydantic import BaseModel

from app.internal.mongodb_connection import connectToDatabase


# _log = idaeslog.getLogger(__name__)
_log = logging.getLogger(__name__)

class Project(BaseModel):
    """Information about a flowsheet."""

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
        _log.info(f'projects is : {self.projects}')

    def addProject(self, document):
        p = Project(
            id_=str(document['_id']),
            name=document['name'],
            description=document['description'],
            state=document['state'],
            history=document['history'],
        )
        self.projects.append(p)
    
data_manager = DataManager()
