import logging
from pathlib import Path
import time
import os
import csv

from typing import Union, List
from pydantic import BaseModel
from bson import ObjectId

from app.internal.mongodb_connection import connectToDatabase
from app.internal.settings import AppSettings
from app.internal.image_handling import generate_download_signed_url_v4


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
    dateCreated: Union[float, None] = None


class DataManager:
    """Manage the active data."""

    VERSION = 1

    def __init__(self, **kwargs) -> None:
        self.app_settings = AppSettings(**kwargs)
        self.db = connectToDatabase()
        # self.projects = []
        # self.fetchProjects()

    def checkForUser(self, user_info):
        _log.info(f"checking for user {user_info}")
        # cursor = self.db.users.find({"email": user_info["email"]})
        # if cursor.count() > 0:
        if self.db.users.count_documents({ "email": user_info["email"] }, limit = 1) > 0:
            _log.info("found user")
            ## TODO: update user data each time they login?
            return True
        else:
            _log.info("did not find user")
            return self.addUser(user_info)

    def addUser(self, user_info):
        _log.info(f"adding user {user_info}")
        user = {
            "email": user_info.get("email", ""),
            "name": user_info.get("name", ""),
            "picture": user_info.get("picture", ""),
            "hd": user_info.get("hd", ""),
            "time_created": time.time(),
        }
        db_response = self.db.users.insert_one(user)
        return db_response

    def fetchProjects(self):
        projects = []
        cursor = self.db.projects.find({})
        for document in cursor:
            projects.append(
                Project(
                    id_=str(document.get("_id", None)),
                    name=document.get("name", ""),
                    description=document.get("description", ""),
                    state=document.get("state", ""),
                    history=document.get("history", []),
                    attributes=document.get("attributes", []),
                    documentType=document.get("documentType", ""),
                    dateCreated=document.get("dateCreated", None),
                )
            )
        return projects

    def createProject(self, project_info):
        ## add timestamp to project
        project_info["dateCreated"] = time.time()
        ## add project to db collection
        _log.info(f"creating project with data: {project_info}")
        db_response = self.db.projects.insert_one(project_info)
        new_id = db_response.inserted_id

        ## add project to project list:
        cursor = self.db.projects.find({"_id": new_id})
        # for document in cursor:
        #     self.addProject(document)

        return str(new_id)

    def fetchProjectData(self, project_id):
        ## get project data
        _id = ObjectId(project_id)
        cursor = self.db.projects.find({"_id": _id})
        project_data = cursor[0]
        project_data["_id"] = str(project_data["_id"])

        ## get project's records
        records = []
        cursor = self.db.records.find({"project_id": project_id})
        for document in cursor:
            # _log.info(f"found document: {document}")
            document["_id"] = str(document["_id"])
            records.append(document)
        return project_data, records

    def fetchRecordData(self, record_id):
        _id = ObjectId(record_id)
        cursor = self.db.records.find({"_id": _id})
        for document in cursor:
            # _log.info(f"found document: {document}")
            document["_id"] = str(document["_id"])
            document["img_url"] = generate_download_signed_url_v4(document["filename"])
            return document
        _log.info(f"RECORD WITH ID {record_id} NOT FOUND")
        return {}

    def createRecord(self, record):
        ## add timestamp to project
        record["dateCreated"] = time.time()
        ## add record to db collection
        db_response = self.db.records.insert_one(record)
        new_id = db_response.inserted_id
        return str(new_id)

    def updateProject(self, project_id, new_data):
        _log.info(f"updating {project_id} to be {new_data}")
        _id = ObjectId(project_id)
        ## need to choose a subset of the data to update. can't update entire record because _id is immutable
        myquery = {"_id": _id}
        newvalues = {"$set": new_data}
        # cursor = self.db.projects.update_one(myquery, newvalues)
        # _log.info(f"successfully updated project? cursor is : {cursor}")
        return "success"

    def updateRecord(self, record_id, new_data):
        # _log.info(f"updating {record_id} to be {new_data}")
        _id = ObjectId(record_id)
        myquery = {"_id": _id}
        newvalues = {"$set": {"attributes": new_data["attributes"]}}
        cursor = self.db.records.update_one(myquery, newvalues)
        return "success"

    def deleteProject(self, project_id):
        _log.info(f"deleting {project_id}")
        _id = ObjectId(project_id)
        myquery = {"_id": _id}
        self.db.projects.delete_one(myquery)
        return "success"

    def deleteRecord(self, record_id):
        _log.info(f"deleting {record_id}")
        _id = ObjectId(record_id)
        myquery = {"_id": _id}
        self.db.records.delete_one(myquery)
        return "success"

    def getProcessor(self, project_id):
        _id = ObjectId(project_id)
        try:
            cursor = self.db.projects.find({"_id": _id})
            document = cursor[0]
            return document["processorId"]
        except Exception as e:
            _log.error(f"unable to find processor id: {e}")
            return None

    def downloadRecords(self, project_id):
        # _log.info(f"downloading records for {project_id}")
        _id = ObjectId(project_id)
        project_cursor = self.db.projects.find({"_id": _id})
        for document in project_cursor:
            keys = document["attributes"]
            project_name = document["name"]
        today = time.time()
        cursor = self.db.records.find({"project_id": project_id})
        record_attributes = []
        for document in cursor:
            record_attribute = {}
            for attribute in keys:
                if attribute in document["attributes"]:
                    record_attribute[attribute] = document["attributes"][attribute][
                        "value"
                    ]
                else:
                    record_attribute[attribute] = "N/A"
            record_attributes.append(record_attribute)

        # compute the output file directory and name
        output_dir = self.app_settings.csv_dir
        output_file = os.path.join(output_dir, f"{project_id}_{today}.csv")
        with open(output_file, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            writer.writeheader()
            writer.writerows(record_attributes)

        return output_file


data_manager = DataManager()
