import argparse
import os
import requests
import time
import shutil


def upload_documents_from_directory(
    backend_url=None,
    user_email=None,
    project_id=None,
    local_directory=None,
    cloud_directory=None,
    delete_local_files=False,
):
    if project_id is None:
        print("please provide a project id (flag -p) to upload documents to")
        return
    if user_email is None:
        print("please provide a contributor's email (flag -e)")
        return
    if local_directory is None and cloud_directory is None:
        print(
            "please provide either a local directory (flag -l) or a cloud directory (flag -c)"
        )
        return
    if backend_url is None:
        # backend_url = f"http://localhost:8001"
        backend_url = f"https://server.uow-carbon.org"
    post_url = f"{backend_url}/upload_document/{project_id}/{user_email}"
    if local_directory is not None:
        files_to_delete = []
        print(f"uploading documents from {local_directory}")
        for subdir, dirs, files in os.walk(local_directory):
            for file in files:
                file_path = os.path.join(subdir, file)
                files_to_delete.append(file_path)

                if ".pdf" in file.lower():
                    mime_type = "application/pdf"
                elif ".tif" in file.lower():
                    mime_type = "image/tiff"
                elif ".png" in file.lower():
                    mime_type = "image/png"
                elif ".jpg" in file.lower():
                    mime_type = "image/jpeg"
                elif ".jpeg" in file.lower():
                    mime_type = "image/jpeg"

                print(f"uploading: {file_path} with mimetype {mime_type}")

                opened_file = open(file_path, "rb")
                upload_files = {
                    "file": (file, opened_file, mime_type),
                    "Content-Disposition": 'form-data; name="file"; filename="'
                    + file
                    + '"',
                    "Content-Type": mime_type,
                }
                requests.post(post_url, files=upload_files)
        if delete_local_files:
            time_to_wait = len(files_to_delete) + 120
            print(f"removing {files_to_delete} in {time_to_wait} seconds")
            time.sleep(time_to_wait)
            try:
                print(f"removing {files_to_delete}")
                shutil.rmtree(local_directory)
            except Exception as e:
                print(f"unable to delete {files_to_delete}: {e}")
            # for each in files_to_delete:
            #     os.remove(each)
    if cloud_directory is not None:
        print(f"uploading documents from {cloud_directory}")
