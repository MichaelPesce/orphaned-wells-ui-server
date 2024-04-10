import logging
from pathlib import Path
import time
import os
import csv
import json
from enum import Enum

from typing import Union, List
from pydantic import BaseModel
from bson import ObjectId
from pymongo import ASCENDING, DESCENDING

from app.internal.mongodb_connection import connectToDatabase
from app.internal.settings import AppSettings
from app.internal.image_handling import generate_download_signed_url_v4


_log = logging.getLogger(__name__)


class Roles(int, Enum):
    """Roles for user accessibility.
    Only approved users should be able to access the app.
    Only special users (admins) should be capable of approving other users.
    """

    pending = -1
    base_user = 1
    admin = 10


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
    creator: Union[str, dict] = ""
    dateCreated: Union[float, None] = None


class DataManager:
    """Manage the active data."""

    VERSION = 1

    def __init__(self, **kwargs) -> None:
        self.app_settings = AppSettings(**kwargs)
        self.db = connectToDatabase()

    def checkForUser(self, user_info, update=True, add=True):
        cursor = self.db.users.find({"email": user_info["email"]})
        foundUser = False
        for document in cursor:
            foundUser = True
            role = document.get("role", Roles.pending)
            if update:
                self.updateUser(user_info)
        if not foundUser and add:
            role = Roles.pending
            self.addUser(user_info, role)
        return role

    def addUser(self, user_info, role=Roles.pending):
        # _log.info(f"adding user {user_info}")
        user = {
            "email": user_info.get("email", ""),
            "name": user_info.get("name", ""),
            "picture": user_info.get("picture", ""),
            "hd": user_info.get("hd", ""),
            "role": role,
            "projects": [],
            "time_created": time.time(),
        }
        db_response = self.db.users.insert_one(user)
        return db_response

    def updateUser(self, user_info):
        # _log.info(f"updating user {user_info}")
        email = user_info.get("email", "")
        user = {
            "name": user_info.get("name", ""),
            "picture": user_info.get("picture", ""),
            "hd": user_info.get("hd", ""),
        }
        myquery = {"email": email}
        newvalues = {"$set": user}
        cursor = self.db.users.update_one(myquery, newvalues)
        return cursor

    def approveUser(self, user_email):
        user = {"role": Roles.base_user}
        myquery = {"email": user_email}
        newvalues = {"$set": user}
        self.db.users.update_one(myquery, newvalues)
        return "success"

    def getUserProjectList(self, user):
        myquery = {"email": user}
        cursor = self.db.users.find(myquery)
        projects = cursor.next().get("projects", [])
        return projects

    def fetchProjects(self, user):
        user_projects = self.getUserProjectList(user)
        projects = []
        cursor = self.db.projects.find({"_id": {"$in": user_projects}})
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
                    creator=document.get("creator", ""),
                    dateCreated=document.get("dateCreated", None),
                )
            )
        return projects

    def createProject(self, project_info, user_info):
        ## add user and timestamp to project
        project_info["creator"] = user_info
        project_info["dateCreated"] = time.time()
        project_info["settings"] = {}
        ## add project to db collection
        # _log.info(f"creating project with data: {project_info}")
        db_response = self.db.projects.insert_one(project_info)
        new_id = db_response.inserted_id

        ## add project to user's project list:
        user_projects = self.getUserProjectList(user_info.get("email", ""))
        myquery = {"email": user_info.get("email", "")}
        user_projects.append(new_id)
        newvalues = {"$set": {"projects": user_projects}}
        self.db.users.update_one(myquery, newvalues)

        return str(new_id)

    def fetchProjectData(self, project_id, user):
        ## get user's projects, check if user has access to this project
        user_projects = self.getUserProjectList(user)
        _id = ObjectId(project_id)
        if not _id in user_projects:
            return None, None

        ## get project data
        cursor = self.db.projects.find({"_id": _id})
        project_data = cursor.next()
        project_data["id_"] = str(project_data["_id"])
        del project_data["_id"]

        ## get project's records
        records = []
        # _log.info(f"checking for records with project_id {project_id}")
        cursor = self.db.records.find({"project_id": project_id}).sort(
            "dateCreated", ASCENDING
        )
        record_index = 1
        for document in cursor:
            document["_id"] = str(document["_id"])
            document["recordIndex"] = record_index
            record_index += 1
            records.append(document)
        return project_data, records

    def fetchRecordData(self, record_id):
        _id = ObjectId(record_id)
        cursor = self.db.records.find({"_id": _id})
        document = cursor.next()
        document["_id"] = str(document["_id"])
        document["img_url"] = generate_download_signed_url_v4(
            document["project_id"], document["filename"]
        )

        ## get project name
        projectId = document.get("project_id", "")
        _id = ObjectId(projectId)
        cursor = self.db.projects.find({"_id": _id})
        project = cursor.next()
        project_name = project.get("name", "")
        document["project_name"] = project_name

        ## get record index
        dateCreated = document.get("dateCreated", 0)
        record_index = self.db.records.count_documents(
            {"dateCreated": {"$lte": dateCreated}, "project_id": projectId}
        )
        document["recordIndex"] = record_index
        return document

    def fetchNextRecord(self, dateCreated, projectId):
        cursor = self.db.records.find(
            {"dateCreated": {"$gt": dateCreated}, "project_id": projectId}
        ).sort("dateCreated", ASCENDING)
        for document in cursor:
            record_id = str(document.get("_id", ""))
            return self.fetchRecordData(record_id)
        cursor = self.db.records.find({"project_id": projectId}).sort(
            "dateCreated", ASCENDING
        )
        document = cursor.next()
        record_id = str(document.get("_id", ""))
        return self.fetchRecordData(record_id)

    def fetchPreviousRecord(self, dateCreated, projectId):
        cursor = self.db.records.find(
            {"dateCreated": {"$lt": dateCreated}, "project_id": projectId}
        ).sort("dateCreated", DESCENDING)
        for document in cursor:
            record_id = str(document.get("_id", ""))
            return self.fetchRecordData(record_id)
        cursor = self.db.records.find({"project_id": projectId}).sort(
            "dateCreated", DESCENDING
        )
        document = cursor.next()
        record_id = str(document.get("_id", ""))
        return self.fetchRecordData(record_id)

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
        self.db.projects.update_one(myquery, newvalues)
        # _log.info(f"successfully updated project? cursor is : {cursor}")
        return "success"
    
    def updateUser(self, email, new_data):
        _log.info(f"updating {email} to be {new_data}")
        ## need to choose a subset of the data to update. can't update entire record because _id is immutable
        myquery = {"email": email}
        newvalues = {"$set": new_data}
        self.db.users.update_one(myquery, newvalues)
        # _log.info(f"successfully updated project? cursor is : {cursor}")
        return "success"

    def updateRecord(self, record_id, new_data, update_type=None):
        # _log.info(f"updating {record_id} to be {new_data}")
        if update_type is None:
            return "failure"
        _id = ObjectId(record_id)
        search_query = {"_id": _id}
        if update_type == "record":
            update_query = {"$set": new_data}
        else:
            update_query = {"$set": {update_type: new_data.get(update_type, None)}}
        self.db.records.update_one(search_query, update_query)
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
            document = cursor.next()
            processor_id = document.get("processorId", None)
            processor_attributes = document.get("attributes", None)
            return processor_id, processor_attributes
        except Exception as e:
            _log.error(f"unable to find processor id: {e}")
            return None

    def downloadRecords(self, project_id, exportType, selectedColumns):
        # _log.info(f"downloading records for {project_id}")
        _id = ObjectId(project_id)
        today = time.time()
        output_dir = self.app_settings.export_dir
        output_file = os.path.join(output_dir, f"{project_id}_{today}.{exportType}")
        project_cursor = self.db.projects.find({"_id": _id})
        attributes = ["file"]
        subattributes = []
        project_document = project_cursor.next()
        for each in project_document.get("attributes", {}):
            if each["name"] in selectedColumns:
                attributes.append(each["name"])
        project_name = project_document.get("name", "")
        cursor = self.db.records.find({"project_id": project_id})
        record_attributes = []
        if exportType == "csv":
            for document in cursor:
                record_attribute = {}
                for attribute in attributes:
                    if attribute in document.get("attributes", []):
                        document_attribute = document["attributes"][attribute]
                        record_attribute[attribute] = document_attribute["value"]

                        ## add subattributes
                        if document_attribute.get("subattributes", None):
                            for subattribute in document_attribute["subattributes"]:
                                document_subattribute = document_attribute[
                                    "subattributes"
                                ][subattribute]
                                record_attribute[subattribute] = document_subattribute[
                                    "value"
                                ]
                                if subattribute not in subattributes:
                                    subattributes.append(subattribute)

                    else:
                        record_attribute[attribute] = "N/A"
                record_attribute["file"] = document.get("filename", "")
                record_attributes.append(record_attribute)

            # compute the output file directory and name
            with open(output_file, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=attributes + subattributes)
                writer.writeheader()
                writer.writerows(record_attributes)
        else:
            for document in cursor:
                record_attribute = {}
                for attribute in attributes:
                    if attribute in document.get("attributes", []):
                        document_attribute = document["attributes"][attribute]
                        record_attribute[attribute] = document_attribute
                    else:
                        record_attribute[attribute] = "N/A"
                record_attribute["file"] = document.get("filename", "")
                record_attributes.append(record_attribute)
            with open(output_file, "w", newline="") as jsonfile:
                json.dump(record_attributes, jsonfile)

        ## update export attributes in project document
        settings = project_document.get("settings", {})
        settings["exportColumns"] = selectedColumns
        update = {"settings": settings}
        self.updateProject(project_id, update)

        return output_file

    def deleteFiles(self, filepaths, sleep_time=5):
        _log.info(f"deleting files: {filepaths} in {sleep_time} seconds")
        time.sleep(sleep_time)
        for filepath in filepaths:
            if os.path.isfile(filepath):
                os.remove(filepath)
                _log.info(f"deleted {filepath}")

    def hasRole(self, user_info, role=Roles.admin):
        email = user_info.get("email", "")
        cursor = self.db.users.find({"email": email})
        try:
            document = cursor.next()
            if document.get("role", Roles.pending) == role:
                return True
            else:
                return False
        except:
            return False

    def getUsers(self, role, includeLowerRoles=True):
        if includeLowerRoles:  # get all users with provided role or lower
            query = {"role": {"$lte": role}}
        else:  # get only users with provided role
            query = {"role": role}
        cursor = self.db.users.find(query)
        users = []
        for document in cursor:
            users.append(
                {
                    "email": document.get("email", ""),
                    "name": document.get("name", ""),
                    "hd": document.get("hd", ""),
                    "picture": document.get("picture", ""),
                    "role": document.get("role", -1),
                }
            )
        return users

    def deleteUser(self, user):
        query = {"email": user}
        delete_response = self.db.users.delete_one(query)
        return user
    
    def addUsersToProject(self, users, project_id):
        _id = ObjectId(project_id)
        try:
            for user in users:
                email = user.get("email", "")
                query = {"email": email}
                cursor = self.db.users.find(query)
                user_object = cursor.next()
                user_projects = user_object.get("projects", [])
                user_projects.append(_id)
                update_query = {"projects": user_projects}
                self.updateUser(email, update_query)
            return {"result": "success"}
        except Exception as e:
            _log.error(f"unable to add users: {e}")
            return {"result": f"{e}"}


data_manager = DataManager()
