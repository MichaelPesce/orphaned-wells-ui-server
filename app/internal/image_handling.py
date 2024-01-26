import asyncio
import logging
import aiofiles
import aiohttp
from PIL import Image, ImageSequence
from gcloud.aio.storage import Storage

_log = logging.getLogger(__name__)


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
    _log(f"uploaded document to cloud storage: {url}")
