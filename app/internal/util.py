import time
import os
import logging
import zipfile
from io import BytesIO
from google.cloud import storage
import datetime
import sys
import requests

_log = logging.getLogger(__name__)
DIRNAME, FILENAME = os.path.split(os.path.abspath(sys.argv[0]))
STORAGE_SERVICE_KEY = os.getenv("STORAGE_SERVICE_KEY")
BUCKET_NAME = os.getenv("STORAGE_BUCKET_NAME")


def sortRecordAttributes(attributes, processor, keep_all_attributes=True):
    processor_attributes = processor["attributes"]

    ## match record attribute to each processor attribute
    sorted_attributes = []
    processor_attributes_list = []
    for each in processor_attributes:
        attribute_name = each["name"]
        processor_attributes_list.append(attribute_name)

        found_ = [item for item in attributes if item["key"] == attribute_name]
        for attribute in found_:
            if attribute is None:
                _log.info(f"{attribute_name} is None")
            else:
                sorted_attributes.append(attribute)

    if keep_all_attributes:
        for attr in attributes:
            attribute_name = attr["key"]
            if attribute_name not in processor_attributes_list:
                _log.debug(
                    f"{attribute_name} was not in processor's attributes. adding this to the end of the sorted attributes list"
                )
                sorted_attributes.append(attr)

    return sorted_attributes


def imageIsValid(image):
    ## some broken records have letters saved where image names should be
    ## for now, just ensure that the name is at least 3 characters long
    try:
        if len(image) > 2:
            return True
        else:
            return False
    except Exception as e:
        _log.error(f"unable to check validity of image: {e}")
        return False


def deleteFiles(filepaths, sleep_time=5):
    _log.info(f"deleting files: {filepaths} in {sleep_time} seconds")
    time.sleep(sleep_time)
    for filepath in filepaths:
        if os.path.isfile(filepath):
            os.remove(filepath)
            _log.info(f"deleted {filepath}")


def validateUser(user):
    ## just make sure that this is a real user with roles
    try:
        if user.get("roles", None):
            return True
        else:
            return False
    except Exception as e:
        _log.error(f"failed attempting to validate user {user}: {e}")


def generate_download_signed_url_v4(
    rg_id, record_id, filename, bucket_name=BUCKET_NAME
):
    """Generates a v4 signed URL for downloading a blob.

    Note that this method requires a service account key file. You can not use
    this if you are using Application Default Credentials from Google Compute
    Engine or from the Google Cloud SDK.
    To generate STORAGE_SERVICE_KEY, follow steps here:
    https://docs.gspread.org/en/latest/oauth2.html#for-bots-using-service-account
    """

    storage_client = storage.Client.from_service_account_json(
        f"{DIRNAME}/internal/{STORAGE_SERVICE_KEY}"
    )

    # blob_name: path to file in google cloud bucket
    blob_name = f"uploads/{rg_id}/{record_id}/{filename}"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v4",
        # This URL is valid for 15 minutes
        expiration=datetime.timedelta(minutes=15),
        # Allow GET requests using this URL.
        method="GET",
    )

    return url


def compileDocumentImageList(records):
    images = {}
    for record in records:
        rg_id = record.get("record_group_id", None)
        record_id = str(record["_id"])
        record_name = record.get("name", record_id)
        image_files = record.get("image_files", [])
        images[record_id] = {
            "files": image_files,
            "rg_id": rg_id,
            "record_id": record_id,
            "record_name": record_name,
        }

    return images


def zip_files(file_paths, documents=None):
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for file_path in file_paths:
            zip_file.write(file_path, os.path.basename(file_path))
        if documents is not None:
            for record_id in documents:
                document = documents[record_id]
                rg_id = document["rg_id"]
                image_files = document["files"]
                record_name = document["record_name"]
                for image_file in image_files:
                    signed_url = generate_download_signed_url_v4(
                        rg_id, record_id, image_file
                    )
                    response = requests.get(signed_url, stream=True)
                    if response.status_code == 200:
                        # Write file content directly into the ZIP archive
                        file_name = os.path.basename(image_file)
                        zip_file.writestr(
                            f"documents/{record_name}/{file_name}", response.content
                        )
                    else:
                        # Handle error and add a placeholder file in the ZIP archive
                        error_message = f"Failed to fetch {signed_url}"
                        zip_file.writestr(
                            f"documents/error_{os.path.basename(image_file)}.txt",
                            error_message,
                        )

    zip_bytes = zip_buffer.getvalue()
    zip_buffer.close()

    return zip_bytes

def searchRecordForAttributeErrors(document):
    attributes = document.get("attributesList", [])
    for attribute in attributes:
        if attribute.get("cleaning_error", False):
            return True
    return False