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
from app.internal.image_handling import (
    generate_download_signed_url_v4,
    delete_google_storage_directory,
)


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
        self.environment = os.getenv("ENVIRONMENT")
        _log.info(f"working in environment: {self.environment}")
        if self.environment == "prod":
            self.backend_url = "https://server.uow-carbon.org"
            os.environ["backend_url"] = "https://server.uow-carbon.org"
        else:
            self.backend_url = "http://localhost:8001"
            os.environ["backend_url"] = "http://localhost:8001"
        
        self.LOCKED = False
        ## user_locks: dictionary that stores user_email, record_id pairs for locked records
        ## record_locks: dictionary that stores record_id, locked_time pairs for locked records
        ## lock_duration: amount of seconds that records remain locked if no changes are made
        self.user_locks = {}
        self.record_locks = {}
        self.lock_duration = 12
    
    def fetchLock(self, user):
        while self.LOCKED and self.LOCKED != user:
            _log.info(f"{user} waiting for lock")
            time.sleep(0.1)
        self.LOCKED = user
        _log.info(f"{user} grabbed lock")
        
    def releaseLock(self, user):
        _log.info(f"{user} releasing lock")
        self.LOCKED = False

    def lockRecord(self, record_id, user):
        _log.info(f"{user} locking {record_id}")

        ## remove any record locks that this user may already have in place
        previously_locked_record_id = self.user_locks.get(user, None)
        if previously_locked_record_id:
            self.releaseRecord(previously_locked_record_id, user)

        self.user_locks[user] = record_id
        self.record_locks[record_id] = time.time()
    
    def releaseRecord(self, record_id=None, user=None):
        if record_id and user:
            _log.info(f"{user} releasing {record_id}")
            del self.user_locks[user]
            del self.record_locks[record_id]
        elif record_id:
            _log.info(f"releasing {record_id}")
            temp_user_locks = {key:val for key, val in self.user_locks.items() if val != record_id}
            self.user_locks = temp_user_locks
            del self.record_locks[record_id]
        elif user:
            _log.info(f"{user} releasing lock")
            record_id = self.user_locks[user]
            del self.user_locks[user]
            del self.record_locks[record_id]

    def getDocument(self, collection, query, clean_id=False, return_list=False):
        try:
            cursor = self.db[collection].find(query)
            if not return_list:
                document = cursor.next()
                if clean_id:
                    document_id = document.get("_id", "")
                    document["_id"] = str(document_id)
                return document
        except Exception as e:
            _log.error(f"unable to find {query} in {collection}: {e}")
            return None

    def checkForUser(self, user_info, update=True, add=True, team="Testing"):
        cursor = self.db.users.find({"email": user_info["email"]})
        foundUser = False
        for document in cursor:
            foundUser = True
            role = document.get("role", Roles.pending)
            if update:
                self.updateUser(user_info)
        if not foundUser and add:
            role = Roles.base_user
            self.addUser(user_info, team, role)
        elif not foundUser and not add:
            role = "not found"
        return role

    def addUser(self, user_info, default_team, role=Roles.pending):
        _log.info(f"adding user {user_info}")
        _log.info(f"team is {default_team}")
        user = {
            "email": user_info.get("email", ""),
            "name": user_info.get("name", ""),
            "picture": user_info.get("picture", ""),
            "hd": user_info.get("hd", ""),
            "role": role,
            "projects": [],
            "time_created": time.time(),
        }
        if default_team is not None:
            user["default_team"] = default_team
            # user["teams"] = [default_team]
        db_response = self.db.users.insert_one(user)

        ## add user to team's users
        team_query = {"name": default_team}
        team_document = self.getDocument("teams", team_query)
        team_users = team_document.get("users", [])
        team_users.append(user_info.get("email", ""))
        newvalues = {"$set": {"users": team_users}}
        self.db.teams.update_one(team_query, newvalues)

        return db_response

    def addUserToTeam(self, email, team, role=Roles.base_user):
        ## CHECK IF USER IS NOT ALREADY ON THIS TEAM
        checkvalues = {"name": team, "users": email}
        found_user = self.db.teams.count_documents(checkvalues)
        if found_user > 0:
            _log.info(f"found {email} on {team}")
            return "already_exists"

        ## update user's teams
        # myquery = {"email": email}
        # newvalues = { "$push": { "teams": team } }
        # cursor = self.db.users.update_one(myquery, newvalues)

        ## update team's users
        myquery = {"name": team}
        newvalues = {"$push": {"users": email}}
        cursor = self.db.teams.update_one(myquery, newvalues)
        return "success"

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
        user_query = {"email": user}
        user_cursor = self.db.users.find(user_query)
        user_document = user_cursor.next()
        default_team = user_document.get("default_team", None)

        team_query = {"name": default_team}
        team_cursor = self.db.teams.find(team_query)
        team_document = team_cursor.next()
        projects = team_document.get("projects", [])
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
        ## get user's default team
        user_email = user_info.get("email", "")
        user_document = self.getDocument("users", ({"email": user_email}))
        default_team = user_document.get("default_team", None)
        if default_team is None:
            ## TODO: handle project creation when a user has no default project
            _log.info(f"user {user_email} has no default team")
            return False

        ## add user and timestamp to project
        project_info["creator"] = user_info
        project_info["team"] = default_team
        project_info["dateCreated"] = time.time()
        project_info["settings"] = {}
        ## add project to db collection
        # _log.info(f"creating project with data: {project_info}")
        db_response = self.db.projects.insert_one(project_info)
        new_project_id = db_response.inserted_id

        ## add project to team's project list:
        team_query = {"name": default_team}
        team_document = self.getDocument("teams", team_query)
        team_projects = team_document.get("projects", [])
        team_projects.append(new_project_id)
        newvalues = {"$set": {"projects": team_projects}}
        self.db.teams.update_one(team_query, newvalues)

        return str(new_project_id)

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

    def getTeamRecords(self, user_info):
        user = user_info.get("email", "")
        ## get user's projects, check if user has access to this project
        user_document = self.getDocument("users", {"email": user})
        default_team = user_document.get("default_team", None)
        team_document = self.getDocument("teams", {"name": default_team})
        projects_list = team_document.get("projects", [])
        records = []
        for _id in projects_list:
            project_id = str(_id)
            ## get project data
            cursor = self.db.projects.find({"_id": _id})
            ## errors out sometimes ?
            try:
                project_data = cursor.next()
                project_data["id_"] = str(project_data["_id"])
                del project_data["_id"]
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
            except Exception as e:
                _log.error(f"unable to add records from project {project_id}: {e}")
        return records

    def fetchRecordData(self, record_id, user_info, direction="next"):
        user = user_info.get("email", "")
        try:
            _id = ObjectId(record_id)
            cursor = self.db.records.find({"_id": _id})
            document = cursor.next()
            document["_id"] = str(document["_id"])
            projectId = document.get("project_id", "")
            project_id = ObjectId(projectId)

            ## check that record is not locked
            self.fetchLock(user)
            locked_time = self.record_locks.get(record_id, None)
            if locked_time is not None:
                ## record is locked. check the time to see if it has expired
                current_time = time.time()
                if locked_time + self.lock_duration > current_time:
                    ## lock is still valid, check if THIS USER has that lock
                    user_has_lock = self.user_locks.get(user, False)
                    if user_has_lock == record_id:
                        ## this user has the lock
                        _log.info(f"this user has lock for this record, continue on")
                    else:
                        ## lock is still valid, different user
                        _log.info(f"lock still valid, moving to {direction} record")
                        self.releaseLock(user)
                        return document, True
                else:
                    ## lock is no longer valid. remove user and record lock for this record
                    _log.info(f"lock has expired, replacing this lock")
                    self.releaseRecord(record_id)
                    self.lockRecord(record_id, user)
            else:
                ## record is not locked. lock it and move on with operation
                _log.info(f"{record_id} is free")
                self.lockRecord(record_id, user)
            self.releaseLock(user)

            user_projects = self.getUserProjectList(user)
            if not project_id in user_projects:
                return None, None
            # document["_id"] = str(document["_id"])
            document["img_url"] = generate_download_signed_url_v4(
                document["project_id"], document["filename"]
            )

            ## get project name
            project = self.getDocument("projects", {"_id": project_id})
            project_name = project.get("name", "")
            document["project_name"] = project_name

            ## get record index
            dateCreated = document.get("dateCreated", 0)
            record_index_query = {
                "dateCreated": {"$lte": dateCreated},
                "project_id": projectId,
            }
            record_index = self.db.records.count_documents(record_index_query)
            document["recordIndex"] = record_index

            return document, False
        except Exception as e:
            _log.error(f"error while fetching record data: {e}")
            self.releaseLock(user)

    def fetchNextRecord(self, dateCreated, projectId, user_info):
        _log.info(f"fetching next record\n{dateCreated}\n{projectId}\n{user_info}")
        cursor = self.db.records.find(
            {"dateCreated": {"$gt": dateCreated}, "project_id": projectId}
        ).sort("dateCreated", ASCENDING)
        for document in cursor:
            record_id = str(document.get("_id", ""))
            return self.fetchRecordData(record_id, user_info, direction="next")
        cursor = self.db.records.find({"project_id": projectId}).sort(
            "dateCreated", ASCENDING
        )
        document = cursor.next()
        record_id = str(document.get("_id", ""))
        return self.fetchRecordData(record_id, user_info)

    def fetchPreviousRecord(self, dateCreated, projectId, user_info):
        cursor = self.db.records.find(
            {"dateCreated": {"$lt": dateCreated}, "project_id": projectId}
        ).sort("dateCreated", DESCENDING)
        for document in cursor:
            record_id = str(document.get("_id", ""))
            return self.fetchRecordData(record_id, user_info, direction="previous")
        cursor = self.db.records.find({"project_id": projectId}).sort(
            "dateCreated", DESCENDING
        )
        document = cursor.next()
        record_id = str(document.get("_id", ""))
        return self.fetchRecordData(record_id, user_info)

    def createRecord(self, record):
        ## add timestamp to project
        record["dateCreated"] = time.time()
        ## add record to db collection
        db_response = self.db.records.insert_one(record)
        new_id = db_response.inserted_id
        return str(new_id)

    def updateProject(self, project_id, new_data):
        _id = ObjectId(project_id)
        ## need to choose a subset of the data to update. can't update entire record because _id is immutable
        myquery = {"_id": _id}
        newvalues = {"$set": new_data}
        self.db.projects.update_one(myquery, newvalues)
        # _log.info(f"successfully updated project? cursor is : {cursor}")
        return "success"

    def updateUserProjects(self, email, new_data):
        _log.info(f"updating {email} to be {new_data}")
        ## need to choose a subset of the data to update. can't update entire record because _id is immutable
        myquery = {"email": email}
        newvalues = {"$set": new_data}
        self.db.users.update_one(myquery, newvalues)
        # _log.info(f"successfully updated project? cursor is : {cursor}")
        return "success"

    def updateRecordReviewStatus(self, record_id, review_status):
        new_data = {"review_status": review_status}
        self.updateRecord(record_id, new_data, "record")

    def updateRecord(self, record_id, new_data, update_type=None):
        ## TODO: make sure that this user has a valid lock for this record before updating
        ## check if they have the lock
        ## if they do, move on.
        ## if they have an expired lock, renew it
        ## if they don't have a lock and no one else does, allow them to update the record and give them the lock (how would we end up here?)
        ## if they don't have a lock and someone else has an invalid one, allow them to update the record and give them the lock
        ## if tehy don't have a lock and someone else has a valid one, navigate this user to the project page
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

    def deleteProject(self, project_id, background_tasks, user_info):
        ## TODO: check if user is a part of the team who owns this project
        _log.info(f"deleting project {project_id}")
        _id = ObjectId(project_id)
        myquery = {"_id": _id}

        ## add to deleted projects collection first
        project_cursor = self.db.projects.find(myquery)
        try:
            project_document = project_cursor.next()
            project_document["deleted_by"] = user_info
            self.db.deleted_projects.insert_one(project_document)
        except Exception as e:
            _log.error(f"unable to add project {project_id} to deleted projects: {e}")

        ## delete from projects collection
        self.db.projects.delete_one(myquery)

        ## add records to deleted records collection and remove from records collection
        background_tasks.add_task(
            self.deleteRecords,
            query={"project_id": project_id},
            deletedBy=user_info,
        )

        ## delete project directory where photos are stored in GCP
        ## hold off on this for now - we may end up wanting to keep these
        # background_tasks.add_task(
        #     delete_google_storage_directory,
        #     project_id=project_id,
        # )
        return "success"

    def deleteRecord(self, record_id):
        ## TODO: check if user is a part of the team who owns the project that owns this record
        _log.info(f"deleting {record_id}")
        _id = ObjectId(record_id)
        myquery = {"_id": _id}
        self.db.records.delete_one(myquery)
        return "success"

    def deleteRecords(self, query, deletedBy):
        _log.info(f"deleting records with query: {query}")
        ## add records to deleted records collection
        record_cursor = self.db.records.find(query)
        try:
            for record_document in record_cursor:
                record_document["deleted_by"] = deletedBy
                self.db.deleted_records.insert_one(record_document)
        except Exception as e:
            _log.error(f"unable to move all deleted records: {e}")

        ## Delete records associated with this project
        self.db.records.delete_many(query)
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
        ## TODO: check if user is a part of the team who owns this project

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

    def getUserInfo(self, email):
        user_document = self.getDocument("users", {"email": email}, clean_id=True)
        return user_document

    def getUsers(
        self, role, user_info, project_id_exclude=None, includeLowerRoles=True
    ):
        ## TODO: accept team id as parameter and use that to determine which users to return
        user = user_info.get("email", "")
        user_document = self.getDocument("users", {"email": user})
        team_id = user_document.get("default_team", None)
        team_document = self.getDocument("teams", {"name": team_id})
        team_users = team_document.get("users", [])
        if includeLowerRoles:  # get all users with provided role or lower
            query = {"role": {"$lte": role}}
        else:  # get only users with provided role
            query = {"role": role}
        cursor = self.db.users.find(query)
        users = []
        if project_id_exclude is not None:
            project_id = ObjectId(project_id_exclude)
            for document in cursor:
                if project_id not in document.get("projects", []):
                    next_user = document.get("email", "")
                    if next_user in team_users:
                        users.append(
                            {
                                "email": document.get("email", ""),
                                "name": document.get("name", ""),
                                "hd": document.get("hd", ""),
                                "picture": document.get("picture", ""),
                                "role": document.get("role", -1),
                            }
                        )
        else:
            for document in cursor:
                next_user = document.get("email", "")
                if next_user in team_users:
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

    def removeUserFromTeam(self, user, team):
        query = {"email": user}
        # delete_response = self.db.users.delete_one(query)
        return user

    def deleteUser(self, email):
        query = {"email": email}
        delete_response = self.db.users.delete_one(query)
        ## TODO: remove user form all teams that include him/her
        query = {"users": email}
        teams_cursor = self.db.teams.find(query)
        for document in teams_cursor:
            team_id = document["_id"]
            user_list = document.get("users", [])
            user_list.remove(email)
            query = {"_id": team_id}
            newvalue = {"$set": {"users": user_list}}
            self.db.teams.update_one(query, newvalue)
        return email

    def addUsersToProject(self, users, project_id):
        ## TODO:
        ## (1) change project to team
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
                self.updateUserProjects(email, update_query)
            return {"result": "success"}
        except Exception as e:
            _log.error(f"unable to add users: {e}")
            return {"result": f"{e}"}

    def checkProjectValidity(self, projectId):
        try:
            project_id = ObjectId(projectId)
        except:
            return False
        project = self.getDocument("projects", {"_id": project_id})
        if project is not None:
            return True


data_manager = DataManager()
