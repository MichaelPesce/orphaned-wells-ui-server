import logging
import time
import os
import csv
import json
import traceback
import re

from bson import ObjectId
from pymongo import ASCENDING, DESCENDING, UpdateOne

from app.internal.mongodb_connection import connectToDatabase
from app.internal.settings import AppSettings
from app.internal.util import generate_download_signed_url_v4
import app.internal.util as util


_log = logging.getLogger(__name__)


class DataManager:
    """Manage the active data."""

    VERSION = 1

    def __init__(self, **kwargs) -> None:
        self.app_settings = AppSettings(**kwargs)
        self.db = connectToDatabase()
        self.environment = os.getenv("ENVIRONMENT")
        _log.info(f"working in environment: {self.environment}")

        self.LOCKED = False
        ## lock_duration: amount of seconds that records remain locked if no changes are made
        self.lock_duration = 120

    ## lock functions
    def fetchLock(self, user):
        ## Can't use variable stored in memory for this
        while self.LOCKED and self.LOCKED != user:
            _log.info(f"{user} waiting for lock")
            time.sleep(0.1)
        self.LOCKED = user
        _log.info(f"{user} grabbed lock")

    def releaseLock(self, user):
        ## Can't use variable stored in memory for this
        _log.info(f"{user} releasing lock")
        self.LOCKED = False

    def lockRecord(self, record_id, user, release_previous_record=True):
        _log.info(f"{user} locking {record_id}")
        if release_previous_record:
            ## remove any record locks that this user may already have in place
            self.releaseRecord(user=user)
        query = {"record_id": record_id}
        data = {
            "user": user,
            "record_id": record_id,
            "timestamp": time.time(),
        }
        self.db.locked_records.update_one(query, {"$set": data}, upsert=True)

    def releaseRecord(self, record_id=None, user=None):
        _log.info(f"releasing record {record_id} or user {user}")
        if record_id:
            self.db.locked_records.delete_many({"record_id": record_id})
        elif user:
            self.db.locked_records.delete_many({"user": user})

    def tryLockingRecord(self, record_id, user):
        try:
            # self.fetchLock(user)
            attained_lock = False
            locked_record_cursor = self.db.locked_records.find({"record_id": record_id})
            record_is_locked = False
            for locked_record_document in locked_record_cursor:
                record_is_locked = True
                break
            locked_record_cursor.close()
            _log.info(f"record_is_locked: {record_is_locked}")
            if record_is_locked:
                ## someone has a lock for this.
                ## (1) check who
                ## (2) check if expired
                locked_time = locked_record_document.get("timestamp", 0)
                lockholder = locked_record_document.get("user", None)
                current_time = time.time()
                if lockholder == user:
                    self.lockRecord(
                        record_id=record_id, user=user, release_previous_record=False
                    )
                    attained_lock = True
                elif locked_time + self.lock_duration < current_time:
                    ## lock is expired
                    self.lockRecord(
                        record_id=record_id, user=user, release_previous_record=True
                    )
                    attained_lock = True
                else:
                    ## lock is still valid by other user
                    attained_lock = False
            else:
                ## record is unlocked, go on ahead
                self.lockRecord(
                    record_id=record_id, user=user, release_previous_record=True
                )
                attained_lock = True
            # self.releaseLock(user)
            return attained_lock
        except Exception as e:
            _log.error(f"error trying to lock record: {e}")
            return False

    ## user functions
    def getUser(self, email):
        cursor = self.db.users.find({"email": email})
        for document in cursor:
            user = document
            user["_id"] = str(user["_id"])
            user["permissions"] = self.getUserPermissions(user)
            return user
        return None

    def updateUserObject(self, user_info):
        cursor = self.db.users.find({"email": user_info["email"]})
        user = None
        for document in cursor:
            user = document
        if user == None:
            return None

        ## update name, picture, hd
        for each in ["name", "picture", "hd"]:
            new_val = user_info.get(each, False)
            if new_val and new_val != "":
                user[each] = new_val

        email = user_info.get("email", "")
        myquery = {"email": email}
        newvalues = {"$set": user}
        self.db.users.update_one(myquery, newvalues)
        return user

    def updateDefaultTeam(self, email, new_team):
        query = {"email": email}
        update = {"$set": {"default_team": new_team}}
        # _log.info(f"{query}, {update}")
        self.db.users.update_one(query, update)

    def addUser(self, user_info, team, team_lead=False, sys_admin=False):
        if team is None:
            _log.error(f"failed to add user {user_info}. team is required")
            return False

        ## assign roles
        roles = {"team": {}, "projects": {}, "system": []}
        if team_lead:
            roles["team"][team] = ["team_lead"]
        else:
            roles["team"][team] = ["team_member"]

        if sys_admin:
            roles["system"].append("sys_admin")
        user = {
            "email": user_info.get("email", ""),
            "name": user_info.get("name", ""),
            "picture": user_info.get("picture", ""),
            "hd": user_info.get("hd", ""),
            "default_team": team,
            "roles": roles,
            "time_created": time.time(),
        }
        db_response = self.db.users.insert_one(user)

        ## add user to team's users
        team_query = {"name": team}
        team_document = self.getDocument("teams", team_query)
        team_users = team_document.get("users", [])
        team_users.append(user_info.get("email", ""))
        newvalues = {"$set": {"users": team_users}}
        self.db.teams.update_one(team_query, newvalues)

        return db_response

    def addUserToTeam(self, email, team):
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

    def updateUserRole(self, email, team, role_category, new_roles):
        try:
            myquery = {"email": email}
            user_doc = self.db.users.find(myquery).next()
            # team = user_doc["default_team"]

            user_roles = user_doc.get("roles", {})
            if role_category == "system":
                if new_roles in user_roles.get("system", []):
                    ## this shouldn't happen
                    _log.info(f"user already has this role")
                    return None
                else:
                    user_roles["system"].append(new_roles)
            elif role_category == "team":
                user_roles["team"][team] = new_roles

            update = {"$set": {"roles": user_roles}}
            cursor = self.db.users.update_one(myquery, update)
            self.recordHistory("updateUser", query=update["$set"])
            return cursor
        except Exception as e:
            _log.error(f"failed to update user role: {e}")
            return e

    def hasPermission(self, email, permission):
        user_doc = self.getUser(email)
        user_permissions = self.getUserPermissions(user_doc)
        if permission in user_permissions:
            return True
        else:
            return False

    def getUserInfo(self, email):
        user_document = self.getDocument("users", {"email": email}, clean_id=True)
        return user_document

    def getUsers(self, user_info):
        user = user_info.get("email", "")
        user_document = self.getDocument("users", {"email": user})
        team_name = user_document.get("default_team", None)
        team_document = self.getDocument("teams", {"name": team_name})
        team_users = team_document.get("users", [])
        cursor = self.db.users.find()
        users = []
        for document in cursor:
            next_user = document.get("email", "")
            if next_user in team_users:
                users.append(
                    {
                        "email": document.get("email", ""),
                        "name": document.get("name", ""),
                        "hd": document.get("hd", ""),
                        "picture": document.get("picture", ""),
                        "roles": document.get("roles", {}),
                    }
                )
        return users

    def removeUserFromTeam(self, user, team):
        query = {"email": user}
        # delete_response = self.db.users.delete_one(query)
        return user

    def deleteUser(self, email, user_info):
        admin_email = user_info.get("email", None)
        query = {"email": email}
        delete_response = self.db.users.delete_one(query)
        ## remove user form all teams that include him/her
        query = {"users": email}
        teams_cursor = self.db.teams.find(query)
        for document in teams_cursor:
            team_id = document["_id"]
            user_list = document.get("users", [])
            user_list.remove(email)
            query = {"_id": team_id}
            newvalue = {"$set": {"users": user_list}}
            self.db.teams.update_one(query, newvalue)
        self.recordHistory("deleteUser", user=admin_email)
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

    ## Fetch/get functions
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

    def getProjectFromRecordGroup(self, rg_id):
        project_cursor = self.db.projects.find({"record_groups": rg_id})
        project_document = project_cursor.next()
        project_document["_id"] = str(project_document["_id"])
        return project_document

    def getTeamProjectList(self, team):
        team_query = {"name": team}
        team_cursor = self.db.teams.find(team_query)
        team_document = team_cursor.next()
        projects = team_document.get("project_list", [])
        for i in range(len(projects)):
            projects[i] = ObjectId(projects[i])
        return projects

    def getUserProjectList(self, user):
        user_query = {"email": user}
        user_cursor = self.db.users.find(user_query)
        user_document = user_cursor.next()
        default_team = user_document.get("default_team", None)
        return self.getTeamProjectList(default_team)

    def getUserRecordGroups(self, user):
        projects = self.fetchProjects(user)
        record_groups = []
        for project in projects:
            record_groups += project.get("record_groups", [])
        return record_groups

    def getRecordGroupProgress(self, rg_id, check_for_errors=True):
        ## get total records count
        query = {"record_group_id": rg_id}
        total_amt = self.db.records.count_documents(query)

        ## get count of reviewed and defective
        query = {
            "record_group_id": rg_id,
            "review_status": {"$in": ["defective", "reviewed"]},
        }
        reviewed_amt = self.db.records.count_documents(query)

        if check_for_errors:
            try:
                query = {
                    "record_group_id": rg_id,
                    "$or": [
                        {
                            "attributesList": {
                                "$elemMatch": {
                                    "$and": [
                                        {"cleaning_error": {"$ne": False}},
                                        {"cleaning_error": {"$exists": True}},
                                    ]
                                }
                            }
                        },
                        {
                            "attributesList.subattributes": {
                                "$elemMatch": {
                                    "$and": [
                                        {"cleaning_error": {"$ne": False}},
                                        {"cleaning_error": {"$exists": True}},
                                    ]
                                }
                            }
                        },
                    ],
                }
                error_amt = self.db.records.count_documents(query)
            except Exception as e:
                _log.info(f"unable to check for errors: {e}")
                error_amt = 0
        else:
            error_amt = 0

        return total_amt, reviewed_amt, error_amt

    def fetchTeamInfo(self, email):
        user_doc = self.db.users.find({"email": email}).next()
        team_name = user_doc["default_team"]
        team_doc = self.db.teams.find({"name": team_name}).next()
        if "projects" in team_doc:
            del team_doc["projects"]
        ## convert object ids to strings
        team_doc["_id"] = str(team_doc["_id"])
        for i in range(len(team_doc["project_list"])):
            project_object_id = team_doc["project_list"][i]
            team_doc["project_list"][i] = str(project_object_id)
        return team_doc

    def fetchTeams(self, user_info):
        email = user_info.get("email", None)
        query = {"users": email}
        teams = []
        teams_cursor = self.db.teams.find(query)
        for document in teams_cursor:
            team_name = document["name"]
            teams.append(team_name)
        return teams

    def fetchProject(self, project_id):
        cursor = self.db.projects.find({"_id": ObjectId(project_id)})
        for document in cursor:
            document["_id"] = str(document["_id"])
            return document
        return None

    def fetchProjects(self, user):
        user_projects = self.getUserProjectList(user)
        projects = []
        cursor = self.db.projects.find({"_id": {"$in": user_projects}})
        for document in cursor:
            document["_id"] = str(document["_id"])
            projects.append(document)
        return projects

    def getProjectRecordGroupsList(self, project_id):
        query = {"_id": ObjectId(project_id)}
        cursor = self.db.projects.find(query)
        document = cursor.next()
        record_groups_list = document.get("record_groups", [])
        return record_groups_list

    def getTeamRecordGroupsList(self, team_name):
        query = {"name": team_name}
        cursor = self.db.teams.find(query)
        document = cursor.next()
        project_list = document.get("project_list", [])
        record_groups_list = []
        for project_id in project_list:
            try:
                rgs = self.getProjectRecordGroupsList(str(project_id))
                record_groups_list += rgs
            except Exception as e:
                _log.error(f"unable to get record groups for project {project_id}: {e}")
        return record_groups_list

    def fetchRecords(
        self,
        sort_by=["dateCreated", 1],
        filter_by={},
        page=None,
        records_per_page=None,
        search_for_errors=True,
    ):
        records = []
        record_index = 1
        if page is not None and records_per_page is not None and records_per_page != -1:
            cursor = (
                self.db.records.find(filter_by)
                .sort(
                    sort_by[0],
                    sort_by[1]
                    # )
                )
                .skip(records_per_page * page)
                .limit(records_per_page)
            )
            record_index += page * records_per_page
        else:
            cursor = self.db.records.find(filter_by).sort(sort_by[0], sort_by[1])

        for document in cursor:
            document["_id"] = str(document["_id"])
            document["recordIndex"] = record_index
            if search_for_errors:
                document["has_errors"] = util.searchRecordForAttributeErrors(document)
            record_index += 1
            records.append(document)
        record_count = self.db.records.count_documents(filter_by)
        return records, record_count

    def fetchRecordsByTeam(
        self,
        user,
        page=None,
        records_per_page=None,
        sort_by=["dateCreated", 1],
        filter_by={},
    ):
        team_info = self.fetchTeamInfo(user["email"])
        rg_list = self.getTeamRecordGroupsList(team_info["name"])
        filter_by["record_group_id"] = {"$in": rg_list}
        return self.fetchRecords(sort_by, filter_by, page, records_per_page)

    def fetchRecordsByRecordGroup(
        self,
        user,
        rg_id,
        page=None,
        records_per_page=None,
        sort_by=["dateCreated", 1],
        filter_by={},
    ):
        filter_by["record_group_id"] = rg_id
        return self.fetchRecords(sort_by, filter_by, page, records_per_page)

    def fetchRecordsByProject(
        self,
        user,
        project_id,
        page=None,
        records_per_page=None,
        sort_by=["dateCreated", 1],
        filter_by={},
    ):
        ## if we arent filtering by record_group_id, add filter to look for ALL record_ids in given project
        if "record_group_id" not in filter_by:
            record_group_ids = self.getProjectRecordGroupsList(project_id)
            filter_by["record_group_id"] = {"$in": record_group_ids}
        return self.fetchRecords(sort_by, filter_by, page, records_per_page)

    def fetchRecordGroups(self, project_id, user):
        project = self.fetchProject(project_id)
        if project is None:
            _log.info(f"project {project_id} not found")
            return {}
        project_record_groups = project.get("record_groups", [])
        record_group_ids = []
        for i in range(len(project_record_groups)):
            record_group_ids.append(ObjectId(project_record_groups[i]))
        record_groups = []
        cursor = self.db.record_groups.find({"_id": {"$in": record_group_ids}})
        for document in cursor:
            document["_id"] = str(document["_id"])
            (
                document["total_amt"],
                document["reviewed_amt"],
                document["error_amt"],
            ) = self.getRecordGroupProgress(document["_id"])
            record_groups.append(document)
        return {"project": project, "record_groups": record_groups}

    def fetchColumnData(self, location, _id):
        if location == "project" or location == "team":
            columns = set()
            if location == "project":
                # get project, set name and settings
                document = self.db.projects.find({"_id": ObjectId(_id)}).next()
                document["_id"] = _id
                # get all record groups
                record_groups = self.getProjectRecordGroupsList(_id)
            else:
                document = self.db.teams.find({"name": _id}).next()
                document["_id"] = str(document["_id"])
                ##TODO: fix object ids in team project list?
                for i in range(len(document["project_list"])):
                    document["project_list"][i] = str(document["project_list"][i])
                record_groups = self.getTeamRecordGroupsList(_id)
            for rg_id in record_groups:
                rg_document = self.db.record_groups.find(
                    {"_id": ObjectId(rg_id)}
                ).next()
                google_id = rg_document["processorId"]
                processor = self.getProcessorByGoogleId(google_id)
                for attr in processor["attributes"]:
                    columns.add(attr["name"])
            columns = list(columns)
            return {"columns": columns, "obj": document}

        elif location == "record_group":
            columns = []
            rg_document = self.db.record_groups.find({"_id": ObjectId(_id)}).next()
            rg_document["_id"] = _id
            google_id = rg_document["processorId"]
            processor = self.getProcessorByGoogleId(google_id)
            for attr in processor["attributes"]:
                columns.append(attr["name"])
            return {"columns": columns, "obj": rg_document}
        return None

    def getProcessorByGoogleId(self, google_id):
        cursor = self.db.processors.find({"processor_id": google_id})
        for document in cursor:
            document["_id"] = str(document["_id"])
            return document
        return None

    def fetchProcessor(self, google_id):
        cursor = self.db.processors.find({"id": google_id})
        for document in cursor:
            document["_id"] = str(document["_id"])
            return document
        return None

    def fetchProcessors(self, user, state):
        processors = []
        cursor = self.db.processors.find({"state": state})
        for document in cursor:
            document["_id"] = str(document["_id"])
            processors.append(document)
        return processors

    def fetchRoles(self, role_category):
        roles = []
        cursor = self.db.roles.find({"category": role_category})
        for document in cursor:
            del document["_id"]
            roles.append(document)
        return roles

    def fetchProjectData(
        self, project_id, user, page, records_per_page, sort_by, filter_by
    ):
        ## get user's projects, check if user has access to this project
        user_projects = self.getUserProjectList(user)
        _id = ObjectId(project_id)
        if not _id in user_projects:
            return None, None

        ## get project data
        cursor = self.db.projects.find({"_id": _id})
        project_data = cursor.next()
        project_data["_id"] = str(project_data["_id"])

        ## get project's records
        records = []
        filter_by["project_id"] = project_id
        record_index = 1
        if page is not None and records_per_page is not None and records_per_page != -1:
            cursor = (
                self.db.records.find(filter_by)
                .sort(
                    sort_by[0],
                    sort_by[1]
                    # )
                )
                .skip(records_per_page * page)
                .limit(records_per_page)
            )
            record_index += page * records_per_page
        else:
            cursor = self.db.records.find(filter_by).sort(sort_by[0], sort_by[1])

        for document in cursor:
            document["_id"] = str(document["_id"])
            document["recordIndex"] = record_index
            record_index += 1
            records.append(document)

        record_count = self.db.records.count_documents(filter_by)
        return project_data, records, record_count

    def fetchRecordGroupData(self, rg_id, user):
        ## get user's projects, check if user has access to this project
        user_record_groups = self.getUserRecordGroups(user)
        if not rg_id in user_record_groups:
            return None, None

        ## get record group data
        _id = ObjectId(rg_id)
        cursor = self.db.record_groups.find({"_id": _id})
        record_group = cursor.next()
        record_group["_id"] = str(record_group["_id"])

        project_document = self.getProjectFromRecordGroup(rg_id)

        return project_document, record_group

    def fetchRecordData(self, record_id, user_info):
        user = user_info.get("email", "")
        _id = ObjectId(record_id)
        cursor = self.db.records.find({"_id": _id})
        try:
            document = cursor.next()
        except:
            _log.error(f"record with id {record_id} does not exist")
            return None, None
        document["_id"] = str(document["_id"])
        rg_id = document.get("record_group_id", "")
        # projectId = document.get("project_id", "")
        # project_id = ObjectId(projectId)

        user_record_groups = self.getUserRecordGroups(user)
        if not rg_id in user_record_groups:
            return None, None

        ## try to attain lock
        attained_lock = self.tryLockingRecord(record_id, user)
        image_urls = []
        for image in document.get("image_files", []):
            if util.imageIsValid(image):
                image_urls.append(
                    generate_download_signed_url_v4(
                        document["record_group_id"], document["_id"], image
                    )
                )
        if len(image_urls) == 0:
            if document.get("filename", False):
                image_urls.append(
                    generate_download_signed_url_v4(
                        document["record_group_id"],
                        document["_id"],
                        document["filename"],
                    )
                )
        document["img_urls"] = image_urls

        ## get record group name
        rg = self.getDocument("record_groups", {"_id": ObjectId(rg_id)})
        rg_name = rg.get("name", "")
        document["rg_name"] = rg_name
        document["rg_id"] = rg_id

        ## get project name
        project_document = self.getProjectFromRecordGroup(rg_id)
        project_name = project_document.get("name", "")
        project_id = str(project_document.get("_id", ""))
        document["project_name"] = project_name
        document["project_id"] = project_id

        ## get record index
        dateCreated = document.get("dateCreated", 0)
        record_index_query = {
            "dateCreated": {"$lte": dateCreated},
            "record_group_id": rg_id,
        }
        record_index = self.db.records.count_documents(record_index_query)
        document["recordIndex"] = record_index

        ## get previous and next IDs
        document["previous_id"] = self.getPreviousRecordId(dateCreated, rg_id)
        document["next_id"] = self.getNextRecordId(dateCreated, rg_id)

        ## sort record attributes
        try:
            google_id = rg["processorId"]
            processor_doc = self.db.processors.find({"google_id": google_id}).next()
            sorted_attributes = util.sortRecordAttributes(
                document["attributesList"], processor_doc
            )
            document["attributesList"] = sorted_attributes
        except Exception as e:
            _log.error(f"unable to sort attributes: {e}")
            _log.error(traceback.format_exc())

        return document, not attained_lock

    def fetchRecordNotes(self, record_id, user_info):
        # user = user_info.get("email", "")
        _id = ObjectId(record_id)
        cursor = self.db.records.find({"_id": _id})
        document = cursor.next()
        return document.get("record_notes", [])

    def getNextRecordId(self, dateCreated, rg_id):
        # _log.info(f"fetching next record for {dateCreated} and {rg_id}")
        cursor = self.db.records.find(
            {"dateCreated": {"$gt": dateCreated}, "record_group_id": rg_id}
        ).sort("dateCreated", ASCENDING)
        for document in cursor:
            record_id = str(document.get("_id", ""))
            return record_id
        cursor = self.db.records.find({"record_group_id": rg_id}).sort(
            "dateCreated", ASCENDING
        )
        document = cursor.next()
        record_id = str(document.get("_id", ""))
        return record_id

    def getPreviousRecordId(self, dateCreated, rg_id):
        # _log.info(f"fetching previous record for {dateCreated} and {rg_id}")
        cursor = self.db.records.find(
            {"dateCreated": {"$lt": dateCreated}, "record_group_id": rg_id}
        ).sort("dateCreated", DESCENDING)
        for document in cursor:
            record_id = str(document.get("_id", ""))
            return record_id
        cursor = self.db.records.find({"record_group_id": rg_id}).sort(
            "dateCreated", DESCENDING
        )
        document = cursor.next()
        record_id = str(document.get("_id", ""))
        return record_id

    def getProcessorByRecordGroupID(self, rg_id):
        _id = ObjectId(rg_id)
        try:
            cursor = self.db.record_groups.find({"_id": _id})
            document = cursor.next()
            google_id = document.get("processorId", None)
            processor_cursor = self.db.processors.find({"google_id": google_id})
            processor_document = processor_cursor.next()
            processor_attributes = processor_document.get("attributes", None)
            return google_id, processor_attributes
        except Exception as e:
            _log.error(f"unable to find processor id: {e}")
            return None

    def getProcessorByRecordID(self, record_id):
        _id = ObjectId(record_id)
        try:
            cursor = self.db.records.find({"_id": _id})
            document = cursor.next()
            rg_id = document["record_group_id"]
            return self.getProcessorByRecordGroupID(rg_id)
        except Exception as e:
            _log.error(f"unable to find processor id: {e}")
            return None

    ## create/add functions
    def createProject(self, project_info, user_info):
        ## get user's default team
        user_email = user_info.get("email", "")
        user_document = self.getDocument("users", ({"email": user_email}))
        default_team = user_document.get("default_team", None)
        if default_team is None:
            _log.info(f"user {user_email} has no default team")
            return False

        ## add default data to project
        project_info["creator"] = user_info
        project_info["team"] = default_team
        project_info["dateCreated"] = time.time()
        project_info["record_groups"] = []
        project_info["history"] = []
        project_info["tags"] = []
        project_info["settings"] = {}

        ## create new project entry
        db_response = self.db.projects.insert_one(project_info)
        new_project_id = db_response.inserted_id

        ## add project to team's project list:
        team_query = {"name": default_team}
        team_document = self.getDocument("teams", team_query)
        team_projects = team_document.get("project_list", [])
        team_projects.append(new_project_id)
        newvalues = {"$set": {"project_list": team_projects}}
        self.db.teams.update_one(team_query, newvalues)

        self.recordHistory("createProject", user_email, str(new_project_id))

        return str(new_project_id)

    def createRecordGroup(self, rg_info, user_info):
        ## get user's default team
        user_email = user_info.get("email", "")
        user_document = self.getDocument("users", ({"email": user_email}))
        default_team = user_document.get("default_team", None)
        if default_team is None:
            _log.info(f"user {user_email} has no default team")
            return False

        ## add user and timestamp to record group
        rg_info["creator"] = user_info
        rg_info["team"] = default_team
        rg_info["dateCreated"] = time.time()
        rg_info["settings"] = {}

        ## get processor id
        processor_document = self.getProcessorByGoogleId(rg_info["processorId"])
        rg_info["processor_id"] = str(processor_document["_id"])

        ## add record group to db collection
        db_response = self.db.record_groups.insert_one(rg_info)
        new_rg_id = db_response.inserted_id

        ## add record group to project's rg list:
        project_query = {"_id": ObjectId(rg_info.get("project_id", None))}
        _log.info(f"project_query: {project_query}")
        project_update = {"$push": {"record_groups": str(new_rg_id)}}

        _log.info(f"project_update: {project_update}")
        self.db.projects.update_one(project_query, project_update)

        self.recordHistory("createRecordGroup", user_email, str(new_rg_id))

        return str(new_rg_id)

    def createRecord(self, record, user_info={}):
        user = user_info.get("email", None)
        ## add timestamp to project
        record["dateCreated"] = time.time()
        ## add record to db collection
        db_response = self.db.records.insert_one(record)
        new_id = db_response.inserted_id
        self.recordHistory("createRecord", user, record_id=str(new_id))
        return str(new_id)

    ## update functions
    def updateProject(self, project_id, new_data, user_info={}):
        user = user_info.get("email", None)
        _id = ObjectId(project_id)
        ## need to choose a subset of the data to update. can't update entire record because _id is immutable
        myquery = {"_id": _id}
        newvalues = {"$set": new_data}
        self.db.projects.update_one(myquery, newvalues)
        self.recordHistory("updateProject", user, project_id)
        cursor = self.db.projects.find(myquery)
        for document in cursor:
            document["_id"] = str(document["_id"])
            return document
        return None

    def updateRecordGroup(self, rg_id, new_data, user_info={}):
        user = user_info.get("email", None)
        _id = ObjectId(rg_id)
        ## need to choose a subset of the data to update. can't update entire record because _id is immutable
        myquery = {"_id": _id}
        newvalues = {"$set": new_data}
        self.db.record_groups.update_one(myquery, newvalues)
        self.recordHistory("updateRecordGroup", user, rg_id)
        cursor = self.db.record_groups.find(myquery)
        for document in cursor:
            document["_id"] = str(document["_id"])
            return document
        return None

    def updateUserProjects(self, email, new_data):
        _log.info(f"updating {email} to be {new_data}")
        ## need to choose a subset of the data to update. can't update entire record because _id is immutable
        myquery = {"email": email}
        newvalues = {"$set": new_data}
        self.db.users.update_one(myquery, newvalues)
        # _log.info(f"successfully updated project? cursor is : {cursor}")
        return "success"

    def updateRecordReviewStatus(self, record_id, review_status, user_info):
        new_data = {"review_status": review_status}
        self.updateRecord(record_id, new_data, "record", user_info)

    def updateRecord(
        self,
        record_id,
        new_data,
        update_type=None,
        field_to_clean=None,
        user_info=None,
        forceUpdate=False,
    ):
        # _log.info(f"updating {record_id} to be {new_data}")
        attained_lock = False
        user = None
        if user_info is None and not forceUpdate:
            return False
        elif user_info is not None:
            user = user_info.get("email", None)
            attained_lock = self.tryLockingRecord(record_id, user)
        if attained_lock or forceUpdate:
            if update_type is None:
                return False
            _id = ObjectId(record_id)
            search_query = {"_id": _id}
            if update_type == "record":
                data_update = new_data
                update_query = {"$set": data_update}
            else:
                data_update = {update_type: new_data.get(update_type, None)}

                ## call cleaning functions
                if field_to_clean:
                    topLevelIndex = field_to_clean["topLevelIndex"]
                    isSubattribute = field_to_clean.get("isSubattribute", False)
                    if isSubattribute:
                        subIndex = field_to_clean["subIndex"]
                        attributeToClean = new_data["attributesList"][topLevelIndex][
                            "subattributes"
                        ][subIndex]
                    else:
                        attributeToClean = new_data["attributesList"][topLevelIndex]
                    # print(attributeToClean)
                    self.cleanAttribute(attributeToClean, record_id=record_id)

                if (
                    update_type == "attributesList"
                    and new_data.get("review_status", None) == "unreviewed"
                ):
                    ## if an attribute is updated and the record is unreviewed, automatically move review_status to incomplete
                    data_update["review_status"] = "incomplete"
                elif update_type == "verification_status" and new_data.get(
                    "review_status", None
                ):
                    data_update["review_status"] = new_data["review_status"]
                elif (
                    update_type == "review_status"
                    and new_data.get("review_status", None) == "unreviewed"
                ):
                    data_update = self.resetRecord(record_id, new_data, user)
                elif (
                    update_type == "review_status"
                    and new_data.get("review_status", None) == "incomplete"
                ):
                    data_update["verification_status"] = None
                elif (
                    update_type == "review_status"
                    and new_data.get("review_status", None) == "defective"
                ):
                    data_update["defective_categories"] = new_data.get(
                        "defective_categories", []
                    )
                    data_update["defective_description"] = new_data.get(
                        "defective_description", None
                    )
                update_query = {"$set": data_update}
            if not forceUpdate:
                ## fetch record's current data so we know what changed in the future
                try:
                    record_doc = self.db.records.find(
                        {"_id": ObjectId(record_id)}
                    ).next()
                    previous_state = {}
                    for each in data_update:
                        previous_state[each] = record_doc.get(each, None)
                except Exception as e:
                    _log.info(f"unable to get record's previous state: {e}")
                    previous_state = None
                self.recordHistory(
                    "updateRecord",
                    user,
                    record_id=record_id,
                    query=data_update,
                    previous_state=previous_state,
                )
            self.db.records.update_one(search_query, update_query)

            return data_update
        else:
            return False

    def updateRecordNotes(self, record_id, data, user_info=None):
        # _log.info(f"updating {record_id} with {data}")
        if user_info is not None:
            user = user_info.get("email", None)
        else:
            user = None
        _id = ObjectId(record_id)
        search_query = {"_id": _id}
        update_type = data["update_type"]
        index = data.get("index", None)
        updates = []
        if update_type == "add":
            ##TODO: check if new index is really new (ie, less than length of list).
            ## in the case that two users simultaneously add notes, there could be a race here
            newNoteText = data["text"]
            isReply = data.get("isReply", False)
            newNote = {
                "text": newNoteText,
                "record_id": record_id,
                "timestamp": time.time(),
                "creator": user,
                "resolved": False,
                "deleted": False,
                "lastUpdated": time.time(),
                "replies": [],
                "isReply": isReply,
                "lastUpdatedUser": user,
            }
            if isReply:
                replyToIndex = data["replyToIndex"]
                newNote["repliesTo"] = replyToIndex
                update1 = {
                    "$push": {
                        "record_notes": newNote,  ## add new note
                    }
                }
                update2 = {
                    "$push": {
                        f"record_notes.{replyToIndex}.replies": index,  ## add index to reply list
                    }
                }
                updates.append(update1)
                updates.append(update2)
            else:
                update = {"$push": {"record_notes": newNote}}
                updates.append(update)
        elif update_type == "edit":
            updatedText = data["text"]
            update = {
                "$set": {
                    f"record_notes.{index}.text": updatedText,
                    f"record_notes.{index}.lastUpdated": time.time(),
                    f"record_notes.{index}.lastUpdatedUser": user,
                }
            }
            updates.append(update)
        elif update_type == "delete":
            update = {
                "$set": {
                    f"record_notes.{index}.deleted": True,
                    f"record_notes.{index}.lastUpdated": time.time(),
                    f"record_notes.{index}.lastUpdatedUser": user,
                }
            }
            updates.append(update)
        elif update_type == "resolve" or update_type == "unresolve":
            new_resolve_value = False
            if update_type == "resolve":
                new_resolve_value = True
            update = {
                "$set": {
                    f"record_notes.{index}.resolved": new_resolve_value,
                    f"record_notes.{index}.lastUpdated": time.time(),
                    f"record_notes.{index}.lastUpdatedUser": user,
                }
            }
            updates.append(update)
        else:
            _log.error(f"invalid update type: {update_type}")
            return None

        for update in updates:
            self.db.records.update_one(search_query, update)
            self.recordHistory(
                "updateRecordNotes", user, record_id=record_id, query=update
            )
        record_doc = self.db.records.find(search_query).next()
        return record_doc.get("record_notes", [])

    def resetRecord(self, record_id, record_data, user):
        print(f"resetting record: {record_id}")
        record_attributes = record_data["attributesList"]
        for attribute in record_attributes:
            attribute_name = attribute["key"]
            original_value = attribute["raw_text"]
            attribute["value"] = original_value
            attribute["confidence"] = attribute["ai_confidence"]
            attribute["edited"] = False
            attribute["cleaning_error"] = False
            attribute["uncleaned_value"] = None
            attribute["cleaned"] = False
            attribute["last_cleaned"] = None
            ## check for subattributes and reset those
            if attribute["subattributes"] is not None:
                record_subattributes = attribute["subattributes"]
                for subattribute in record_subattributes:
                    original_value = subattribute["raw_text"]
                    subattribute["value"] = original_value
                    subattribute["confidence"] = subattribute.get("ai_confidence", None)
                    subattribute["edited"] = False
                    subattribute["cleaning_error"] = False
                    subattribute["uncleaned_value"] = None
                    subattribute["cleaned"] = False
                    subattribute["last_cleaned"] = None
        update = {
            "review_status": "unreviewed",
            "attributesList": record_attributes,
            "verification_status": None,
        }
        # history is recorded in the function that calls this
        return update

    ## delete functions
    def deleteProject(self, project_id, background_tasks, user_info):
        ## TODO: check if user is a part of the team who owns this project
        _log.info(f"deleting project {project_id}")
        _id = ObjectId(project_id)
        myquery = {"_id": _id}

        ## add to deleted projects collection first
        project_cursor = self.db.projects.find(myquery)
        project_document = project_cursor.next()
        project_document["deleted_by"] = user_info
        team = project_document.get("team", "")
        self.db.deleted_projects.insert_one(project_document)

        ## delete from projects collection
        self.db.projects.delete_one(myquery)

        ## delete record groups
        record_groups = project_document.get("record_groups", [])
        self.deleteRecordGroups(record_groups=record_groups, deletedBy=user_info)

        ## add records to deleted records collection and remove from records collection
        background_tasks.add_task(
            self.deleteRecords,
            query={"record_group_id": {"$in": record_groups}},
            deletedBy=user_info,
        )

        self.recordHistory(
            "deleteProject", user_info.get("email", None), project_id=project_id
        )

        self.removeProjectFromTeam(_id, team)
        return "success"

    def deleteRecordGroup(self, rg_id, background_tasks, user_info):
        _log.info(f"deleting record group {rg_id}")
        _id = ObjectId(rg_id)
        myquery = {"_id": _id}

        ## add to deleted record groups collection first
        record_group_cursor = self.db.record_groups.find(myquery)
        record_group_doc = record_group_cursor.next()
        record_group_doc["deleted_by"] = user_info
        team = record_group_doc.get("team", "")
        self.db.deleted_record_groups.insert_one(record_group_doc)

        ## delete from record groups collection
        self.db.record_groups.delete_one(myquery)

        ## add records to deleted records collection and remove from records collection
        background_tasks.add_task(
            self.deleteRecords,
            query={"record_group": rg_id},
            deletedBy=user_info,
        )

        self.recordHistory(
            "deleteRecordGroup", user_info.get("email", None), rg_id=rg_id
        )

        ## remove from project list
        self.removeRecordGroupFromProject(rg_id)

        self.removeRecordGroupFromTeam(_id, team)
        return "success"

    def deleteRecord(self, record_id, user_info):
        user = user_info.get("email", None)
        ## TODO: check if user is a part of the team who owns the project that owns this record
        _log.info(f"deleting {record_id}")
        _id = ObjectId(record_id)
        myquery = {"_id": _id}
        self.db.records.delete_one(myquery)
        self.recordHistory("deleteRecord", user=user, record_id=record_id)
        return "success"

    def deleteRecords(self, query, deletedBy):
        user = deletedBy.get("email", None)
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
        # self.recordHistory("deleteRecords", user=user, notes=query)
        return "success"

    def deleteRecordGroups(self, record_groups, deletedBy):
        user = deletedBy.get("email", None)
        _log.info(f"deleting record groups: {record_groups}")
        record_group_ids = []
        for i in range(len(record_groups)):
            record_group_ids.append(ObjectId(record_groups[i]))
        ## add to deleted records collection
        query = {"_id": {"$in": record_group_ids}}
        cursor = self.db.record_groups.find(query)
        try:
            for document in cursor:
                document["deleted_by"] = deletedBy
                self.db.deleted_record_groups.insert_one(document)
        except Exception as e:
            _log.error(f"unable to move all deleted record groups: {e}")

        ## Delete records associated with this project
        self.db.record_groups.delete_many(query)
        return "success"

    def removeProjectFromTeam(self, project_id, team):
        team_query = {"name": team}
        update = {"$pull": {"project_list": project_id}}
        self.db.teams.update_many(team_query, update)

    def removeRecordGroupFromProject(self, rg_id):
        query = {"record_groups": rg_id}
        update = {"$pull": {"record_groups": rg_id}}
        self.db.projects.update_many(query, update)

    def removeRecordGroupFromTeam(self, rg_id, team):
        team_query = {"name": team}
        update = {"$pull": {"record_groups": rg_id}}
        self.db.teams.update_many(team_query, update)

    ## miscellaneous functions
    def downloadRecords(
        self,
        records,
        exportType,
        user_info,
        _id,
        location,
        selectedColumns=[],
        keep_all_columns=False,
        output_filename=None,
    ):
        user = user_info.get("email", None)
        ## TODO: check if user is a part of the team who owns this project
        today = time.time()
        output_dir = self.app_settings.export_dir
        if output_filename is None:
            output_file = os.path.join(output_dir, f"{_id}_{today}.{exportType}")
        else:
            output_file = f"{output_filename}.{exportType}"
        attributes = ["file"]
        subattributes = []
        record_attributes = []
        if exportType == "csv":
            for document in records:
                document_id = str(document["_id"])
                try:
                    current_attributes = set()
                    record_attribute = {}
                    for document_attribute in document["attributesList"]:
                        attribute_name = document_attribute["key"].replace(" ", "")
                        if attribute_name in selectedColumns or keep_all_columns:
                            original_attribute_name = attribute_name
                            i = 2
                            while attribute_name in current_attributes:
                                ## add a number to the end of the attribute so it (and its subattributes)
                                ## is differentiable from other instances of the attribute
                                attribute_name = f"{original_attribute_name}_{i}"
                                i += 1
                            current_attributes.add(attribute_name)
                            if attribute_name not in attributes:
                                attributes.append(attribute_name)
                            record_attribute[attribute_name] = document_attribute[
                                "value"
                            ]
                            ## add subattributes
                            if document_attribute.get("subattributes", None):
                                for document_subattribute in document_attribute[
                                    "subattributes"
                                ]:
                                    subattribute_name = f"{attribute_name}[{document_subattribute['key']}]"
                                    record_attribute[
                                        subattribute_name
                                    ] = document_subattribute["value"]
                                    if subattribute_name not in subattributes:
                                        subattributes.append(subattribute_name)
                    record_attribute["file"] = document.get("filename", "")
                    record_attributes.append(record_attribute)
                except Exception as e:
                    _log.info(f"unable to add {document_id}: {e}")

            # compute the output file directory and name
            with open(output_file, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=attributes + subattributes)
                writer.writeheader()
                writer.writerows(record_attributes)
        else:
            for document in records:
                document_id = str(document["_id"])
                try:
                    record_attribute = {}
                    for document_attribute in document["attributesList"]:
                        attribute_name = document_attribute["key"]
                        if attribute_name in selectedColumns or keep_all_columns:
                            record_attribute[attribute_name] = document_attribute
                    record_attribute["file"] = document.get("filename", "")
                    record_attributes.append(record_attribute)
                except Exception as e:
                    _log.info(f"unable to add {document_id}: {e}")
            with open(output_file, "w", newline="") as jsonfile:
                json.dump(record_attributes, jsonfile)

        if location == "project":
            self.recordHistory("downloadRecords", user=user, project_id=_id)
        elif location == "record_group":
            self.recordHistory("downloadRecords", user=user, rg_id=_id)
        elif location == "team":
            self.recordHistory(
                "downloadRecords", user=user, notes="downloaded team records"
            )
        return output_file

    def getUserPermissions(self, user):
        user_team = user["default_team"]
        roles = user.get("roles", {})

        user_roles = []
        ## get system role
        for role in roles.get("system", []):
            user_roles.append(role)
        ## get team roles
        for role in roles.get("team", {}).get(user_team, []):
            user_roles.append(role)

        ## compile permissions from each role
        query = {"id": {"$in": user_roles}}
        role_cursor = self.db.roles.find(query)
        user_permissions = set()
        for each in role_cursor:
            for perm in each["permissions"]:
                user_permissions.add(perm)

        return list(user_permissions)

    def checkProjectValidity(self, projectId):
        try:
            project_id = ObjectId(projectId)
        except:
            return False
        project = self.getDocument("projects", {"_id": project_id})
        if project is not None:
            return True

    def checkIfRecordExists(self, filename, rg_id):
        ## remove file extension
        filename = filename.split(".")[0]

        ## query database
        query = {"filename": {"$regex": filename}, "record_group_id": rg_id}
        found_document = self.db.records.count_documents(query)
        if found_document > 0:
            return True
        else:
            return False

    def checkIfRecordsExist(self, filenames, rg_id):
        # Convert filenames into regex patterns
        regex_patterns = [
            {"filename": {"$regex": re.escape(filename.split(".")[0]), "$options": "i"}}
            for filename in filenames
        ]
        query = {
            "$and": [
                {"record_group_id": rg_id},  # Match the given rg_id
                {"$or": regex_patterns},  # Match any filename in filenames as regex
            ]
        }
        record_cursor = self.db.records.find(query)
        duplicate_records = set()
        for document in record_cursor:
            duplicate_records.add(document["filename"].split(".")[0])
        return list(duplicate_records)

    def checkRecordGroupValidity(self, rg_id):
        try:
            rg_id = ObjectId(rg_id)
        except:
            return False
        rg = self.getDocument("record_groups", {"_id": rg_id})
        if rg is not None:
            return True

    def recordHistory(
        self,
        action,
        user=None,
        project_id=None,
        rg_id=None,
        record_id=None,
        notes=None,
        query=None,
        previous_state=None,
    ):
        try:
            history_item = {
                "action": action,
                "user": user,
                "project_id": project_id,
                "record_group_id": rg_id,
                "record_id": record_id,
                "notes": notes,
                "query": query,
                "previous_state": previous_state,
                "timestamp": time.time(),
            }
            self.db.history.insert_one(history_item)
        except Exception as e:
            _log.error(f"unable to record history item: {e}")

    def cleanAttribute(self, attribute, record_id=None, rg_id=None):
        if record_id is None and rg_id is None:
            return None
        if rg_id is not None:
            _, processor_attributes = self.getProcessorByRecordGroupID(rg_id)
        else:
            _, processor_attributes = self.getProcessorByRecordID(record_id)

        ## convert processor attributes to dict
        processor_attributes = util.convert_processor_attributes_to_dict(
            processor_attributes
        )

        if attribute.get("isSubattribute", False):
            parentAttribute = attribute.get("topLevelAttribute", "")
            subattributeKey = attribute["key"]
            subattribute_identifier = f"{parentAttribute}::{subattributeKey}"
            util.cleanRecordAttribute(
                processor_attributes=processor_attributes,
                attribute=attribute,
                subattributeKey=subattribute_identifier,
            )
        else:
            util.cleanRecordAttribute(
                processor_attributes=processor_attributes, attribute=attribute
            )

    def cleanCollection(self, location, _id):
        documents = []
        try:
            if location == "record":
                _log.info(f"cleaning record {_id}")
                _, processor_attributes = self.getProcessorByRecordID(_id)
                object_id = ObjectId(_id)
                query = {"_id": object_id}
                documents.append(self.db.records.find(query).next())
            elif location == "record_group":
                _log.info(f"cleaning record group {_id}")
                _, processor_attributes = self.getProcessorByRecordGroupID(_id)
                cursor = self.db.records.find({"record_group_id": _id})
                for each in cursor:
                    documents.append(each)
            else:
                _log.error(f"clean {location} is not supported")
                return False

            ## convert processor attributes to dict
            processor_attributes = util.convert_processor_attributes_to_dict(
                processor_attributes
            )
            util.cleanRecords(
                processor_attributes=processor_attributes, documents=documents
            )
            update_ops = []
            for document in documents:
                update_ops.append(
                    UpdateOne({"_id": document["_id"]}, {"$set": document})
                )
            _log.info(f"updateOps length {len(update_ops)}")
            self.db.records.bulk_write(update_ops)
        except Exception as e:
            _log.error(f"error on cleaning {location}: {e}")


data_manager = DataManager()
