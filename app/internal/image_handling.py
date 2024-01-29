import os
import logging
import datetime
import json
import aiofiles
import aiohttp
from PIL import Image
from gcloud.aio.storage import Storage
from google.api_core.client_options import ClientOptions
from google.cloud import documentai, storage
from dotenv import load_dotenv

_log = logging.getLogger(__name__)

load_dotenv()
LOCATION = os.getenv("LOCATION")
PROJECT_ID = os.getenv("PROJECT_ID")
PROCESSOR_ID = os.getenv("PROCESSOR_ID")
STORAGE_SERVICE_KEY = os.getenv("STORAGE_SERVICE_KEY")
os.environ["GCLOUD_PROJECT"] = PROJECT_ID

docai_client = documentai.DocumentProcessorServiceClient(
    client_options=ClientOptions(api_endpoint=f"{LOCATION}-documentai.googleapis.com")
)
RESOURCE_NAME = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)


def convert_tiff(filename, file_ext, output_directory, convert_to=".png"):
    # print(f'converting: {filename}.{file_ext} to {convert_to}')
    filepath = f"{output_directory}/{filename}{file_ext}"
    try:
        outfile = f"{output_directory}/{filename}{convert_to}"
        # print(f'outfile is {outfile}')
        try:
            im = Image.open(filepath)
            im.thumbnail(im.size)
            im.save(outfile, "PNG", quality=100)
            return outfile
        except Exception as e:
            print(f"unable to save {filename}: {e}")
            return filepath

    except Exception as e:
        print(f"failed to convert {filename}: {e}")
        return filepath


## Document AI functions
def process_image(file_path, file_name, mime_type):
    with open(file_path, "rb") as image:
        image_content = image.read()

    raw_document = documentai.RawDocument(content=image_content, mime_type=mime_type)
    request = documentai.ProcessRequest(name=RESOURCE_NAME, raw_document=raw_document)

    # Use the Document AI client to process the document
    result = docai_client.process_document(request=request)
    document_object = result.document

    # our predefined attributes will be located in the entities object
    document_entities = document_object.entities
    """
    entities has the following (useful) attributes: 
    <attribute>: <example value>
    type_: "Spud_Date"
    mention_text: "8-25-72"
    confidence: 1
    normalized_value:
        {
            date_value {
                year: 1972
                month: 8
                day: 25
            }
            text: "1972-08-25"
        }
    """
    attributes = {}
    for entity in document_entities:
        # print(f"found entity: {entity}")
        attribute = entity.type_
        confidence = entity.confidence
        raw_text = entity.mention_text
        # gotta do something with this; it shows up for each attribute but only need it for specific ones (date)
        normalized_value = entity.normalized_value
        attributes[attribute] = {
            "confidence": confidence,
            "raw_text": raw_text,
            "value": raw_text,
            # "normalized_value": normalized_value,
        }
    return attributes


## Google Cloud Storage Functions
async def async_upload_to_bucket(
    blob_name, file_obj, folder="uploads", bucket_name="uploaded_documents_v0"
):
    """Upload image file to bucket."""
    async with aiohttp.ClientSession() as session:
        storage = Storage(service_file="./internal/creds.json", session=session)
        status = await storage.upload(bucket_name, f"{folder}/{blob_name}", file_obj)
        return status["selfLink"]


async def upload_to_google_storage(file_path, file_name):
    async with aiofiles.open(file_path, "rb") as afp:
        f = await afp.read()
    url = await async_upload_to_bucket(file_name, f)
    _log.info(f"uploaded document to cloud storage: {url}")


def generate_download_signed_url_v4(filename, bucket_name="uploaded_documents_v0"):
    """Generates a v4 signed URL for downloading a blob.

    Note that this method requires a service account key file. You can not use
    this if you are using Application Default Credentials from Google Compute
    Engine or from the Google Cloud SDK.
    To generate STORAGE_SERVICE_KEY, follow steps here:
    https://docs.gspread.org/en/latest/oauth2.html#for-bots-using-service-account
    """

    storage_client = storage.Client.from_service_account_json(
        f"./internal/{STORAGE_SERVICE_KEY}"
    )

    # blob_name: path to file in google cloud bucket
    blob_name = f"uploads/{filename}"
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
