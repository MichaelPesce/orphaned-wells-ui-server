"""
image_handling.py

Handles uploaded documents:
- safely stages local files
- converts PDFs/TIFFs to display images
- uploads display images to storage
- creates initial records
- runs Document AI, either per-document or in a GCS batch
"""
import logging
import mimetypes
import multiprocessing
import os
import re
import shutil
import tempfile
import time
import tracemalloc
import uuid
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import unquote

import aiofiles
import fitz
from fastapi import HTTPException
from PIL import Image

from ogrre.internal import document_ai_api
from ogrre.internal import storage_api
from ogrre.internal.whitespace_detector import detect_whitespace_from_bytes

import ogrre.internal.util as util

_log = logging.getLogger(__name__)

DETECT_WHITESPACE = os.getenv("DETECT_WHITESPACE", "true").lower() in (
    "1",
    "true",
    "yes",
)
MEMORY_PROFILE = os.getenv("MEMORY_PROFILE", "").lower() in ("1", "true", "yes")
MEMORY_PROFILE_RATE = int(os.getenv("MEMORY_PROFILE_RATE", "1"))
MEMORY_PROFILE_TOP = int(os.getenv("MEMORY_PROFILE_TOP", "10"))
PROCESS_IMAGE_IN_SUBPROCESS = os.getenv("PROCESS_IMAGE_IN_SUBPROCESS", "").lower() in (
    "1",
    "true",
    "yes",
)
DOCUMENT_AI_BATCH_MAX_FILES = int(os.getenv("DOCUMENT_AI_BATCH_MAX_FILES", "1000"))
DOCUMENT_AI_BATCH_TIMEOUT = int(os.getenv("DOCUMENT_AI_BATCH_TIMEOUT", "3600"))
DOCUMENT_AI_BATCH_CLEANUP = os.getenv("DOCUMENT_AI_BATCH_CLEANUP", "true").lower() in (
    "1",
    "true",
    "yes",
)
MAX_ZIP_UNCOMPRESSED_BYTES = int(
    os.getenv("MAX_ZIP_UNCOMPRESSED_BYTES", str(250 * 1024 * 1024))
)

SUPPORTED_DOCUMENT_MIME_TYPES = {
    "application/pdf",
    "image/jpeg",
    "image/png",
    "image/tiff",
}
SUPPORTED_DOCUMENT_EXTENSIONS = {
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".tif": "image/tiff",
    ".tiff": "image/tiff",
}
SAFE_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._ -]+")

_profile_counter = 0


@dataclass
class PreparedDocument:
    record_id: str
    rg_id: str
    file_name: str
    original_filename: str
    mime_type: str
    doc_ai_input_path: str
    output_paths: List[str]
    image_file_names: List[str]
    files_to_delete: List[str]
    processing_dir: str


def _maybe_take_snapshot():
    global _profile_counter
    if not MEMORY_PROFILE:
        return None
    _profile_counter += 1
    if _profile_counter % MEMORY_PROFILE_RATE != 0:
        return None
    if not tracemalloc.is_tracing():
        tracemalloc.start()
    return tracemalloc.take_snapshot()


def _log_snapshot_diff(before, after, label, record_id=None):
    if not before or not after:
        return
    pid = os.getpid()
    prefix = f"pid={pid}"
    if record_id:
        prefix = f"{prefix} record_id={record_id}"
    stats = after.compare_to(before, "lineno")
    _log.info(f"memory profile {label} [{prefix}]: top {MEMORY_PROFILE_TOP}")
    for stat in stats[:MEMORY_PROFILE_TOP]:
        _log.info(f"{stat}")


def _sanitize_filename(filename: Optional[str]) -> str:
    basename = os.path.basename((filename or "").replace("\\", "/")).strip()
    basename = SAFE_FILENAME_PATTERN.sub("_", basename)
    basename = basename.strip(" .")
    return basename or f"upload-{uuid.uuid4().hex}"


def _dedupe_filename(filename: str, used_names: set) -> str:
    stem, suffix = os.path.splitext(filename)
    candidate = filename
    counter = 2
    while candidate.lower() in used_names:
        candidate = f"{stem}_{counter}{suffix}"
        counter += 1
    used_names.add(candidate.lower())
    return candidate


def _duplicate_record_key(filename: str) -> str:
    return os.path.basename(filename).split(".")[0]


def _find_duplicate_candidate_filenames(candidates, data_manager, rg_id):
    if not candidates:
        return set()

    existing_record_keys = set(
        data_manager.checkIfRecordsExist(
            [candidate["filename"] for candidate in candidates], rg_id
        )
    )
    if not existing_record_keys:
        return set()

    return {
        candidate["filename"]
        for candidate in candidates
        if _duplicate_record_key(candidate["filename"]) in existing_record_keys
    }


def _make_processing_dir(image_dir: str, prefix: str = "upload_") -> str:
    os.makedirs(image_dir, exist_ok=True)
    return tempfile.mkdtemp(prefix=prefix, dir=image_dir)


def _get_supported_mime_type(filename: str, content_type: Optional[str] = None):
    normalized_content_type = (content_type or "").split(";")[0].lower()
    if normalized_content_type in SUPPORTED_DOCUMENT_MIME_TYPES:
        return normalized_content_type

    guessed_mime_type = mimetypes.guess_type(filename)[0]
    if guessed_mime_type in SUPPORTED_DOCUMENT_MIME_TYPES:
        return guessed_mime_type

    return SUPPORTED_DOCUMENT_EXTENSIONS.get(Path(filename).suffix.lower())


async def _save_upload_file(upload_file, destination_path):
    async with aiofiles.open(destination_path, "wb") as out_file:
        chunk_size = 1024 * 1024
        while True:
            chunk = await upload_file.read(chunk_size)
            if not chunk:
                break
            await out_file.write(chunk)


def _cleanup_local_files(filepaths: List[str]):
    filepaths = list(dict.fromkeys(filepaths or []))
    parent_dirs = {str(Path(filepath).parent) for filepath in filepaths}
    util.deleteFiles(filepaths=filepaths, sleep_time=0)
    for parent_dir in sorted(parent_dirs, key=len, reverse=True):
        dirname = os.path.basename(parent_dir)
        if not dirname.startswith(("upload_", "zip_")):
            continue
        try:
            os.rmdir(parent_dir)
        except OSError:
            pass


def _cleanup_prepared_documents(prepared_documents: List[PreparedDocument]):
    for prepared_document in prepared_documents:
        _cleanup_local_files(prepared_document.files_to_delete)


def _parse_api_number(filename: str):
    try:
        return int(filename.split("_")[0])
    except Exception:
        _log.info("unable to parse api number")
        return None


def _blank_attribute(key: str):
    return {
        "key": key,
        "ai_confidence": None,
        "confidence": None,
        "raw_text": "",
        "text_value": "",
        "value": "",
        "normalized_vertices": None,
        "normalized_value": None,
        "subattributes": None,
        "isSubattribute": False,
        "edited": False,
        "page": None,
    }


def _sort_and_clean_attributes(
    attributes_list,
    processor_attributes,
    run_cleaning_functions=True,
):
    if not processor_attributes:
        _log.info("no processor attributes found")
        processor_attributes = []

    processor_attributes_dictionary = {}
    if run_cleaning_functions:
        processor_attributes_dictionary = util.convert_processor_attributes_to_dict(
            processor_attributes
        )

    found_attributes = {}
    for idx, attribute in enumerate(attributes_list):
        attribute_key = attribute["key"]
        found_attributes.setdefault(attribute_key, []).append(idx)
        if not run_cleaning_functions:
            continue

        util.cleanRecordAttribute(
            processor_attributes=processor_attributes_dictionary,
            attribute=attribute,
        )
        for subattribute in attribute.get("subattributes") or []:
            util.cleanRecordAttribute(
                processor_attributes=processor_attributes_dictionary,
                attribute=subattribute,
                subattributeKey=f"{attribute_key}::{subattribute['key']}",
            )

    sorted_attributes_list = []
    processor_attributes_list = []
    for processor_attribute in processor_attributes:
        attr = processor_attribute["name"]
        processor_attributes_list.append(attr)
        if attr in found_attributes:
            for idx in found_attributes[attr]:
                sorted_attributes_list.append(attributes_list[idx])
        elif "::" not in attr:
            sorted_attributes_list.append(_blank_attribute(attr))

    for attr in found_attributes:
        if attr in processor_attributes_list:
            continue
        _log.info(
            f"{attr} was not in processor's attributes. adding this to the end of the sorted attributes list"
        )
        for idx in found_attributes[attr]:
            sorted_attributes_list.append(attributes_list[idx])

    return sorted_attributes_list


def _mark_record_error(record_id, rg_id, file_name, data_manager, error, source):
    error_message = str(error)
    _log.error(f"{source}: {error_message}")
    record = {
        "record_group_id": rg_id,
        "filename": f"{file_name}",
        "status": "error",
        "error_message": error_message,
    }
    data_manager.updateRecord(
        record_id,
        record,
        update_type="record",
        forceUpdate=True,
        calling_function=source,
    )


def _update_record_with_attributes(
    record_id,
    rg_id,
    file_name,
    attributes_list,
    processor_attributes,
    data_manager,
    reprocessed=False,
    run_cleaning_functions=True,
    calling_function="process_image",
):
    sorted_attributes_list = _sort_and_clean_attributes(
        attributes_list,
        processor_attributes,
        run_cleaning_functions=run_cleaning_functions,
    )
    record = {
        "record_group_id": rg_id,
        "attributesList": sorted_attributes_list,
        "filename": f"{file_name}",
        "status": "reprocessed" if reprocessed else "digitized",
    }
    data_manager.updateRecord(
        record_id,
        record,
        update_type="record",
        forceUpdate=True,
        calling_function=calling_function,
    )
    _log.info(f"updated record in db: {record_id}")
    return record_id


def _get_record_image_uploads(prepared_document: PreparedDocument):
    uploads = []
    for output_path, file_name in zip(
        prepared_document.output_paths, prepared_document.image_file_names
    ):
        uploads.append(
            {
                "file_path": output_path,
                "key": (
                    f"uploads/{prepared_document.rg_id}/"
                    f"{prepared_document.record_id}/{file_name}"
                ),
                "content_type": _get_supported_mime_type(file_name),
            }
        )
    return uploads


def _detect_record_image_whitespace(prepared_document: PreparedDocument):
    whitespace_results = []
    for output_path, file_name in zip(
        prepared_document.output_paths, prepared_document.image_file_names
    ):
        if not DETECT_WHITESPACE:
            continue

        with open(output_path, "rb") as file_handle:
            file_bytes = file_handle.read()

        try:
            result = detect_whitespace_from_bytes(
                file_bytes, min_whitespace_pct=99.99
            )
            whitespace_results.append(
                {
                    "is_mostly_whitespace": result.get("meets_threshold"),
                    "whitespace_pct": result.get("whitespace_pct"),
                    "threshold": result.get("threshold"),
                    "total_pixels": result.get("total_pixels"),
                    "white_pixels": result.get("white_pixels"),
                    "error": None,
                }
            )
        except Exception as e:
            whitespace_results.append(
                {
                    "is_mostly_whitespace": None,
                    "whitespace_pct": None,
                    "threshold": None,
                    "total_pixels": None,
                    "white_pixels": None,
                    "error": str(e),
                }
            )
        del file_bytes
    return whitespace_results


def _raise_for_upload_failures(upload_results):
    failures = [result["error"] for result in upload_results if result.get("error")]
    if failures:
        raise RuntimeError("; ".join(str(failure) for failure in failures))


def _upload_record_images(prepared_document: PreparedDocument, data_manager):
    whitespace_results = _detect_record_image_whitespace(prepared_document)
    upload_results = storage_api.upload_file_paths(
        _get_record_image_uploads(prepared_document)
    )
    _raise_for_upload_failures(upload_results)

    if DETECT_WHITESPACE:
        data_manager.updateRecordInternal(
            prepared_document.record_id, "image_whitespace", whitespace_results
        )


def _upload_record_images_batch(
    prepared_documents: List[PreparedDocument], data_manager
):
    upload_items = []
    upload_documents = []
    whitespace_by_record = {}
    failures_by_record = {}

    for prepared_document in prepared_documents:
        try:
            whitespace_by_record[
                prepared_document.record_id
            ] = _detect_record_image_whitespace(prepared_document)
            document_uploads = _get_record_image_uploads(prepared_document)
            upload_items.extend(document_uploads)
            upload_documents.extend([prepared_document] * len(document_uploads))
        except Exception as e:
            failures_by_record[prepared_document.record_id] = e

    upload_results = storage_api.upload_file_paths(upload_items)
    for upload_result, prepared_document in zip(upload_results, upload_documents):
        if (
            upload_result.get("error")
            and prepared_document.record_id not in failures_by_record
        ):
            failures_by_record[prepared_document.record_id] = upload_result["error"]

    ready_documents = []
    for prepared_document in prepared_documents:
        failure = failures_by_record.get(prepared_document.record_id)
        if failure:
            _mark_record_error(
                prepared_document.record_id,
                prepared_document.rg_id,
                prepared_document.file_name,
                data_manager,
                failure,
                "upload_record_images",
            )
            _cleanup_local_files(prepared_document.files_to_delete)
            continue

        if DETECT_WHITESPACE:
            data_manager.updateRecordInternal(
                prepared_document.record_id,
                "image_whitespace",
                whitespace_by_record.get(prepared_document.record_id, []),
            )
        ready_documents.append(prepared_document)

    return ready_documents


def convert_pdf_file(input_path, output_directory, output_stem, convert_to=".png"):
    try:
        output_paths = []
        dpi = 100
        doc = fitz.open(input_path)
        zoom = 4
        mat = fitz.Matrix(zoom, zoom)

        try:
            for idx, page in enumerate(doc):
                suffix = "" if idx == 0 else f"_{idx + 1}"
                outfile = os.path.join(
                    output_directory, f"{output_stem}{suffix}{convert_to}"
                )
                pix = page.get_pixmap(matrix=mat, dpi=dpi)
                pix.save(outfile)
                output_paths.append(outfile)
        finally:
            doc.close()

        return output_paths or [input_path]
    except Exception as e:
        _log.error(f"failed to convert {input_path}: {e}")
        return [input_path]


def convert_tiff_file(input_path, output_directory, output_stem, convert_to=".png"):
    try:
        outfile = os.path.join(output_directory, f"{output_stem}{convert_to}")
        with Image.open(input_path) as image:
            image.thumbnail(image.size)
            image.save(outfile, "PNG", quality=100)
        return [outfile]
    except Exception as e:
        _log.error(f"failed to convert {input_path}: {e}")
        return [input_path]


def _prepare_document_record(
    rg_id,
    user_info,
    original_output_path,
    original_filename,
    data_manager,
    mime_type,
):
    filename, file_ext = os.path.splitext(original_filename)
    output_directory = str(Path(original_output_path).parent)
    normalized_ext = file_ext.lower()

    if normalized_ext in (".tif", ".tiff"):
        output_paths = convert_tiff_file(
            original_output_path, output_directory, filename
        )
    elif normalized_ext == ".pdf":
        output_paths = convert_pdf_file(
            original_output_path, output_directory, filename
        )
    else:
        output_paths = [original_output_path]

    image_file_names = [Path(output_path).name for output_path in output_paths]
    record_file_ext = Path(image_file_names[0]).suffix or file_ext
    file_name = f"{filename}{record_file_ext}"

    new_record = {
        "record_group_id": rg_id,
        "name": filename,
        "filename": file_name,
        "api_number": _parse_api_number(filename),
        "contributor": user_info,
        "status": "processing",
        "review_status": "unreviewed",
        "original_filename": original_filename,
        "image_files": image_file_names,
    }
    new_record_id = data_manager.createRecord(new_record, user_info)
    files_to_delete = list(dict.fromkeys(output_paths + [original_output_path]))

    return PreparedDocument(
        record_id=new_record_id,
        rg_id=rg_id,
        file_name=file_name,
        original_filename=original_filename,
        mime_type=mime_type,
        doc_ai_input_path=original_output_path,
        output_paths=output_paths,
        image_file_names=image_file_names,
        files_to_delete=files_to_delete,
        processing_dir=output_directory,
    )


def _schedule_document_ai_processing(
    prepared_document,
    background_tasks,
    processor_id,
    model_id,
    processor_attributes,
    data_manager,
    reprocessed=False,
    run_cleaning_functions=True,
    undeployProcessor=True,
):
    background_tasks.add_task(
        process_prepared_document,
        prepared_document=prepared_document,
        processor_id=processor_id,
        model_id=model_id,
        processor_attributes=processor_attributes,
        data_manager=data_manager,
        reprocessed=reprocessed,
        run_cleaning_functions=run_cleaning_functions,
        undeployProcessor=undeployProcessor,
    )


def process_prepared_document(
    prepared_document,
    processor_id,
    model_id,
    processor_attributes,
    data_manager,
    reprocessed=False,
    run_cleaning_functions=True,
    undeployProcessor=True,
):
    try:
        _upload_record_images(prepared_document, data_manager)
    except Exception as e:
        _mark_record_error(
            prepared_document.record_id,
            prepared_document.rg_id,
            prepared_document.file_name,
            data_manager,
            e,
            "upload_record_images",
        )
        _cleanup_local_files(prepared_document.files_to_delete)
        return

    task_kwargs = {
        "file_name": prepared_document.file_name,
        "mime_type": prepared_document.mime_type,
        "rg_id": prepared_document.rg_id,
        "record_id": prepared_document.record_id,
        "processor_id": processor_id,
        "model_id": model_id,
        "processor_attributes": processor_attributes,
        "doc_ai_input_path": prepared_document.doc_ai_input_path,
        "reprocessed": reprocessed,
        "files_to_delete": prepared_document.files_to_delete,
        "run_cleaning_functions": run_cleaning_functions,
        "undeployProcessor": undeployProcessor,
    }
    if PROCESS_IMAGE_IN_SUBPROCESS:
        _spawn_process_image_worker(**task_kwargs)
    else:
        process_image(data_manager=data_manager, **task_kwargs)


async def process_single_file(
    rg_id,
    user_info,
    background_tasks,
    file,
    original_output_path=None,
    file_ext=None,
    filename=None,
    data_manager=None,
    reprocessed=False,
    run_cleaning_functions=True,
    undeployProcessor=True,
):
    if data_manager is None:
        raise HTTPException(500, detail="data manager is required")

    safe_filename = _sanitize_filename(file.filename)
    mime_type = _get_supported_mime_type(safe_filename, file.content_type)
    if not mime_type:
        raise HTTPException(400, detail=f"Unsupported file type: {safe_filename}")

    processing_dir = _make_processing_dir(data_manager.app_settings.img_dir)
    original_output_path = os.path.join(processing_dir, safe_filename)

    try:
        await _save_upload_file(file, original_output_path)
        filename, file_ext = os.path.splitext(safe_filename)
        return process_document(
            rg_id,
            user_info,
            background_tasks,
            original_output_path,
            file_ext,
            filename,
            data_manager,
            mime_type,
            doc_ai_input_path=original_output_path,
            original_filename=safe_filename,
            reprocessed=reprocessed,
            run_cleaning_functions=run_cleaning_functions,
            undeployProcessor=undeployProcessor,
        )
    except HTTPException:
        raise
    except Exception as e:
        _log.error(f"unable to read image file: {e}")
        _cleanup_local_files([original_output_path])
        raise HTTPException(400, detail=f"Unable to process image file: {e}")


def process_document(
    rg_id,
    user_info,
    background_tasks,
    original_output_path,
    file_ext,
    filename,
    data_manager,
    mime_type,
    doc_ai_input_path,
    original_filename=None,
    reprocessed=False,
    run_cleaning_functions=True,
    undeployProcessor=True,
):
    original_filename = original_filename or f"{filename}{file_ext}"
    prepared_document = _prepare_document_record(
        rg_id=rg_id,
        user_info=user_info,
        original_output_path=original_output_path,
        original_filename=original_filename,
        data_manager=data_manager,
        mime_type=mime_type,
    )

    (
        processor_id,
        model_id,
        processor_attributes,
    ) = data_manager.getProcessorByRecordGroupID(rg_id)
    _schedule_document_ai_processing(
        prepared_document=prepared_document,
        background_tasks=background_tasks,
        processor_id=processor_id,
        model_id=model_id,
        processor_attributes=processor_attributes,
        data_manager=data_manager,
        reprocessed=reprocessed,
        run_cleaning_functions=run_cleaning_functions,
        undeployProcessor=undeployProcessor,
    )
    return {"record_id": prepared_document.record_id}


async def process_upload_files_batch(
    rg_id,
    user_info,
    background_tasks,
    files,
    data_manager,
    preventDuplicates=True,
    reprocessed=False,
    run_cleaning_functions=True,
    undeployProcessor=True,
):
    if len(files) > DOCUMENT_AI_BATCH_MAX_FILES:
        raise HTTPException(
            400,
            detail=f"Document AI batch processing supports up to {DOCUMENT_AI_BATCH_MAX_FILES} files per request.",
        )

    candidates = []
    skipped = []
    used_names = set()

    for upload_file in files:
        safe_filename = _dedupe_filename(
            _sanitize_filename(upload_file.filename), used_names
        )
        mime_type = _get_supported_mime_type(safe_filename, upload_file.content_type)
        if not mime_type:
            skipped.append({"filename": safe_filename, "reason": "unsupported_type"})
            continue

        processing_dir = _make_processing_dir(data_manager.app_settings.img_dir)
        destination_path = os.path.join(processing_dir, safe_filename)
        await _save_upload_file(upload_file, destination_path)
        candidates.append(
            {
                "path": destination_path,
                "filename": safe_filename,
                "mime_type": mime_type,
            }
        )

    return process_local_documents_batch(
        rg_id=rg_id,
        user_info=user_info,
        background_tasks=background_tasks,
        candidates=candidates,
        data_manager=data_manager,
        preventDuplicates=preventDuplicates,
        reprocessed=reprocessed,
        run_cleaning_functions=run_cleaning_functions,
        undeployProcessor=undeployProcessor,
        skipped=skipped,
    )


def _extract_supported_zip_documents(zip_file, image_dir, zip_filename):
    extract_dir = _make_processing_dir(image_dir, prefix="zip_")
    candidates = []
    skipped = []
    used_names = set()

    try:
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            file_infos = [info for info in zip_ref.infolist() if not info.is_dir()]
            total_uncompressed_bytes = sum(info.file_size for info in file_infos)
            if total_uncompressed_bytes > MAX_ZIP_UNCOMPRESSED_BYTES:
                raise HTTPException(
                    413,
                    detail=(
                        f"Zip contents are too large after extraction "
                        f"({total_uncompressed_bytes} bytes)."
                    ),
                )
            if len(file_infos) > DOCUMENT_AI_BATCH_MAX_FILES:
                raise HTTPException(
                    400,
                    detail=f"Zip contains more than {DOCUMENT_AI_BATCH_MAX_FILES} files.",
                )

            for info in file_infos:
                if info.filename.startswith("__MACOSX/"):
                    continue
                safe_filename = _dedupe_filename(
                    _sanitize_filename(Path(info.filename).name), used_names
                )
                mime_type = _get_supported_mime_type(safe_filename)
                if not mime_type:
                    skipped.append(
                        {"filename": safe_filename, "reason": "unsupported_type"}
                    )
                    continue

                destination_path = os.path.join(extract_dir, safe_filename)
                with zip_ref.open(info) as source_file:
                    with open(destination_path, "wb") as destination_file:
                        shutil.copyfileobj(source_file, destination_file)

                candidates.append(
                    {
                        "path": destination_path,
                        "filename": safe_filename,
                        "mime_type": mime_type,
                    }
                )
    except HTTPException:
        raise
    except zipfile.BadZipFile as e:
        _cleanup_local_files([os.path.join(extract_dir, "placeholder")])
        raise HTTPException(400, detail=f"Invalid zip file: {e}")

    _log.info(
        f"processing zip {zip_filename}: {len(candidates)} supported documents, {len(skipped)} skipped"
    )
    if not candidates:
        _cleanup_local_files([os.path.join(extract_dir, "placeholder")])
    return candidates, skipped


def process_zip(
    rg_id,
    user_info,
    background_tasks,
    zip_file,
    image_dir,
    zip_filename,
    backend_url=None,
    data_manager=None,
    preventDuplicates=True,
    reprocessed=False,
    run_cleaning_functions=True,
    undeployProcessor=True,
):
    if data_manager is None:
        raise HTTPException(500, detail="data manager is required")

    candidates, skipped = _extract_supported_zip_documents(
        zip_file.file, image_dir, zip_filename
    )
    return process_local_documents_batch(
        rg_id=rg_id,
        user_info=user_info,
        background_tasks=background_tasks,
        candidates=candidates,
        data_manager=data_manager,
        preventDuplicates=preventDuplicates,
        reprocessed=reprocessed,
        run_cleaning_functions=run_cleaning_functions,
        undeployProcessor=undeployProcessor,
        skipped=skipped,
    )


def process_local_documents_batch(
    rg_id,
    user_info,
    background_tasks,
    candidates,
    data_manager,
    preventDuplicates=True,
    reprocessed=False,
    run_cleaning_functions=True,
    undeployProcessor=True,
    skipped=None,
):
    skipped = skipped or []
    duplicate_files = []
    prepared_documents = []
    files_to_cleanup = []
    batch_id = uuid.uuid4().hex
    duplicate_candidate_filenames = set()
    if preventDuplicates:
        duplicate_candidate_filenames = _find_duplicate_candidate_filenames(
            candidates, data_manager, rg_id
        )

    for candidate in candidates:
        filename = candidate["filename"]
        candidate_path = candidate["path"]
        if filename in duplicate_candidate_filenames:
            duplicate_files.append(filename)
            files_to_cleanup.append(candidate_path)
            continue

        try:
            prepared_documents.append(
                _prepare_document_record(
                    rg_id=rg_id,
                    user_info=user_info,
                    original_output_path=candidate_path,
                    original_filename=filename,
                    data_manager=data_manager,
                    mime_type=candidate["mime_type"],
                )
            )
        except Exception as e:
            _log.error(f"unable to prepare {filename}: {e}")
            skipped.append({"filename": filename, "reason": str(e)})
            files_to_cleanup.append(candidate_path)

    _cleanup_local_files(files_to_cleanup)

    if prepared_documents:
        (
            processor_id,
            model_id,
            processor_attributes,
        ) = data_manager.getProcessorByRecordGroupID(rg_id)
        background_tasks.add_task(
            process_document_batch,
            prepared_documents=prepared_documents,
            rg_id=rg_id,
            processor_id=processor_id,
            model_id=model_id,
            processor_attributes=processor_attributes,
            data_manager=data_manager,
            reprocessed=reprocessed,
            run_cleaning_functions=run_cleaning_functions,
            undeployProcessor=undeployProcessor,
            batch_id=batch_id,
        )

    return {
        "batch_id": batch_id,
        "record_ids": [
            prepared_document.record_id for prepared_document in prepared_documents
        ],
        "duplicates": duplicate_files,
        "skipped": skipped,
    }


def _can_use_document_ai_batch():
    return (
        document_ai_api.supports_batch_processing() and storage_api.is_google_storage()
    )


def process_document_batch(
    prepared_documents,
    rg_id,
    processor_id,
    model_id,
    processor_attributes,
    data_manager,
    reprocessed=False,
    run_cleaning_functions=True,
    undeployProcessor=True,
    batch_id=None,
):
    ready_documents = _upload_record_images_batch(prepared_documents, data_manager)
    if not ready_documents:
        return

    if not _can_use_document_ai_batch():
        _log.info(
            "Document AI batch processing unavailable; falling back to per-document processing"
        )
        for prepared_document in ready_documents:
            process_image(
                file_name=prepared_document.file_name,
                mime_type=prepared_document.mime_type,
                rg_id=rg_id,
                record_id=prepared_document.record_id,
                processor_id=processor_id,
                model_id=model_id,
                processor_attributes=processor_attributes,
                data_manager=data_manager,
                doc_ai_input_path=prepared_document.doc_ai_input_path,
                reprocessed=reprocessed,
                files_to_delete=prepared_document.files_to_delete,
                run_cleaning_functions=run_cleaning_functions,
                undeployProcessor=undeployProcessor,
            )
        return

    try:
        _process_documents_with_google_batch(
            prepared_documents=ready_documents,
            rg_id=rg_id,
            processor_id=processor_id,
            model_id=model_id,
            processor_attributes=processor_attributes,
            data_manager=data_manager,
            reprocessed=reprocessed,
            run_cleaning_functions=run_cleaning_functions,
            batch_id=batch_id or uuid.uuid4().hex,
        )
    finally:
        _cleanup_prepared_documents(ready_documents)


def _process_documents_with_google_batch(
    prepared_documents,
    rg_id,
    processor_id,
    model_id,
    processor_attributes,
    data_manager,
    reprocessed=False,
    run_cleaning_functions=True,
    batch_id=None,
):
    input_prefix = f"document_ai/batch_input/{batch_id}"
    output_prefix = f"document_ai/batch_output/{batch_id}/"
    output_gcs_uri = storage_api.make_gcs_uri(output_prefix)
    gcs_documents = []
    document_by_gcs_uri: Dict[str, PreparedDocument] = {}
    document_by_input_key: Dict[str, PreparedDocument] = {}
    upload_items = []

    try:
        for prepared_document in prepared_documents:
            input_ext = Path(prepared_document.original_filename).suffix.lower()
            input_key = f"{input_prefix}/{prepared_document.record_id}{input_ext}"
            upload_items.append(
                {
                    "file_path": prepared_document.doc_ai_input_path,
                    "key": input_key,
                    "content_type": prepared_document.mime_type,
                }
            )
            document_by_input_key[input_key] = prepared_document

        upload_results = storage_api.upload_file_paths(upload_items)
        for upload_result in upload_results:
            prepared_document = document_by_input_key[upload_result["key"]]
            if upload_result.get("error"):
                _mark_record_error(
                    prepared_document.record_id,
                    rg_id,
                    prepared_document.file_name,
                    data_manager,
                    upload_result["error"],
                    "stage_document_ai_batch_input",
                )
                continue

            gcs_uri = upload_result["gcs_uri"]
            gcs_documents.append(
                {
                    "gcs_uri": gcs_uri,
                    "mime_type": prepared_document.mime_type,
                }
            )
            document_by_gcs_uri[gcs_uri] = prepared_document
            document_by_gcs_uri[unquote(gcs_uri)] = prepared_document

        if not gcs_documents:
            return

        process_statuses = document_ai_api.batch_process_gcs_documents(
            gcs_documents=gcs_documents,
            processor_id=processor_id,
            model_id=model_id,
            gcs_output_uri=output_gcs_uri,
            timeout=DOCUMENT_AI_BATCH_TIMEOUT,
        )

        completed_record_ids = set()
        for process_status in process_statuses:
            prepared_document = document_by_gcs_uri.get(
                process_status["input_gcs_source"]
            ) or document_by_gcs_uri.get(unquote(process_status["input_gcs_source"]))
            if prepared_document is None:
                _log.error(
                    f"Document AI returned an unknown input source: {process_status['input_gcs_source']}"
                )
                continue

            completed_record_ids.add(prepared_document.record_id)
            if process_status["status_code"] != 0:
                _mark_record_error(
                    prepared_document.record_id,
                    rg_id,
                    prepared_document.file_name,
                    data_manager,
                    process_status["status_message"],
                    "process_document_batch",
                )
                continue

            try:
                attributes_list = _attributes_from_output_destination(
                    process_status["output_gcs_destination"],
                    using_default_processor=data_manager.using_default_processor,
                )
                _update_record_with_attributes(
                    prepared_document.record_id,
                    rg_id,
                    prepared_document.file_name,
                    attributes_list,
                    processor_attributes,
                    data_manager,
                    reprocessed=reprocessed,
                    run_cleaning_functions=run_cleaning_functions,
                    calling_function="process_document_batch",
                )
            except Exception as e:
                _mark_record_error(
                    prepared_document.record_id,
                    rg_id,
                    prepared_document.file_name,
                    data_manager,
                    e,
                    "process_document_batch",
                )

        for prepared_document in prepared_documents:
            if prepared_document.record_id not in completed_record_ids:
                _mark_record_error(
                    prepared_document.record_id,
                    rg_id,
                    prepared_document.file_name,
                    data_manager,
                    "Document AI did not return a status for this document",
                    "process_document_batch",
                )
    except Exception as e:
        for prepared_document in prepared_documents:
            _mark_record_error(
                prepared_document.record_id,
                rg_id,
                prepared_document.file_name,
                data_manager,
                e,
                "process_document_batch",
            )
    finally:
        if DOCUMENT_AI_BATCH_CLEANUP:
            try:
                storage_api.delete_gcs_uri_prefix(
                    storage_api.make_gcs_uri(input_prefix)
                )
                storage_api.delete_gcs_uri_prefix(output_gcs_uri)
            except Exception as e:
                _log.info(f"unable to clean Document AI batch staging files: {e}")


def _attributes_from_output_destination(
    output_gcs_destination, using_default_processor
):
    if not output_gcs_destination:
        raise ValueError("Document AI did not provide an output destination")

    output_bucket, _ = storage_api.parse_gcs_uri(output_gcs_destination)
    output_blob_names = storage_api.list_gcs_uri(output_gcs_destination)
    json_blob_names = sorted(
        blob_name for blob_name in output_blob_names if blob_name.endswith(".json")
    )
    if not json_blob_names:
        raise ValueError("Document AI produced no JSON output")

    attributes_list = []
    for blob_name in json_blob_names:
        document_bytes = storage_api.download_file_bytes(
            key=blob_name, bucket_name=output_bucket
        )
        attributes_list.extend(
            document_ai_api.attributes_from_document_json(
                document_bytes,
                using_default_processor=using_default_processor,
            )
        )
    return attributes_list


def _process_image_worker(**kwargs):
    from ogrre.internal.data_manager import DataManager

    data_manager = DataManager()
    process_image(data_manager=data_manager, **kwargs)


def _spawn_process_image_worker(**kwargs):
    ctx = multiprocessing.get_context("spawn")
    process = ctx.Process(target=_process_image_worker, kwargs=kwargs)
    process.daemon = True
    process.start()


def process_image(
    file_name,
    mime_type,
    rg_id,
    record_id,
    processor_id,
    model_id,
    processor_attributes,
    data_manager,
    doc_ai_input_path,
    reprocessed=False,
    files_to_delete=None,
    run_cleaning_functions=True,
    undeployProcessor=True,
):
    files_to_delete = files_to_delete or []
    snapshot_start = _maybe_take_snapshot()
    try:
        with open(doc_ai_input_path, "rb") as file_handle:
            image_content = file_handle.read()
    except Exception as e:
        _mark_record_error(
            record_id, rg_id, file_name, data_manager, e, "process_image"
        )
        _cleanup_local_files(files_to_delete)
        return

    snapshot_after_prepare = _maybe_take_snapshot()
    _log_snapshot_diff(
        snapshot_start,
        snapshot_after_prepare,
        "after_prepare_request",
        record_id=record_id,
    )

    try:
        attributes_list = document_ai_api.process_document_content(
            image_content=image_content,
            mime_type=mime_type,
            processor_id=processor_id,
            model_id=model_id,
            using_default_processor=data_manager.using_default_processor,
        )
    except Exception as e:
        _mark_record_error(
            record_id, rg_id, file_name, data_manager, e, "process_image"
        )
        _cleanup_local_files(files_to_delete)
        return
    finally:
        del image_content

    snapshot_after_process = _maybe_take_snapshot()
    _log_snapshot_diff(
        snapshot_after_prepare,
        snapshot_after_process,
        "after_docai_process",
        record_id=record_id,
    )
    _log.info("processed document in doc_ai")

    record_id = _update_record_with_attributes(
        record_id,
        rg_id,
        file_name,
        attributes_list,
        processor_attributes,
        data_manager,
        reprocessed=reprocessed,
        run_cleaning_functions=run_cleaning_functions,
        calling_function="process_image",
    )
    _cleanup_local_files(files_to_delete)
    return record_id


def deployProcessor(rg_id, data_manager):
    _log.debug(f"attempting to deploy processor for record group {rg_id}")
    start_time = time.time()
    deployment = document_ai_api.deploy_processor(rg_id, data_manager)
    if deployment != "DEPLOYED":
        finish_time = time.time()
        _log.error(
            f"we have an issue, deployment failed. took {finish_time-start_time} seconds to fail deploy"
        )
        return False
    finish_time = time.time()
    _log.debug(f"took {finish_time-start_time} seconds to DEPLOY")
    return True


def undeployProcessor(rg_id, data_manager):
    _log.debug(f"attempting to undeploy processor for record group {rg_id}")
    start_time = time.time()
    document_ai_api.undeploy_processor(rg_id, data_manager)
    finish_time = time.time()
    _log.debug(f"took {finish_time-start_time} seconds to undeploy")
    return True


def check_if_processor_is_deployed(rg_id, data_manager):
    try:
        return document_ai_api.check_if_processor_is_deployed(rg_id, data_manager)
    except Exception as e:
        _log.error(f"unable to check processor status: {e}")
        return 10
