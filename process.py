#!/usr/bin/env python3
"""
GCS-only image thumbnail processor.

This script:
- Reads INPUT_FOLDER environment variable which must be a GCS path (gs://bucket[/prefix]).
- Lists objects under that prefix and identifies images (by content-type if present, otherwise by extension).
- Splits the total images across TASK_COUNT and TASK_INDEX (environment variables).
- For images assigned to this task, downloads each image, creates a 100x100 thumbnail (preserving aspect ratio),
  and uploads the thumbnail to the root of the same bucket in a timestamped folder:
    gs://<bucket>/<TIMESTAMP>/<original_filename>
  The timestamp format includes year, month, day, hour (no seconds or minute): YYYYMMDDTHHZ

Environment:
- INPUT_FOLDER (required): gs://bucket[/optional/prefix]
- CLOUD_RUN_TASK_INDEX (optional, default 0)
- CLOUD_RUN_TASK_COUNT (optional, default 1)

Dependencies:
- google-cloud-storage
- Pillow
"""

from __future__ import annotations

import datetime
import io
import math
import mimetypes
import os
import sys
from typing import List, Tuple

from google.cloud import storage
from PIL import Image

# Configuration
TASK_INDEX = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0"))
TASK_COUNT = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1"))
INPUT_FOLDER = os.environ.get("INPUT_FOLDER")

# Heuristic image file extensions
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif", ".webp"}

# GCS client
_storage_client = storage.Client()


def is_gs_path(path: str) -> bool:
    return isinstance(path, str) and path.startswith("gs://")


def parse_gs_path(gs_path: str) -> Tuple[str, str]:
    """
    Parse a GCS path like gs://bucket/prefix into (bucket, prefix_without_slashes).
    The prefix returned will not have a leading or trailing slash. It may be empty.
    """
    if not is_gs_path(gs_path):
        raise ValueError("gs_path must start with 'gs://'")
    without = gs_path[5:]
    parts = without.split("/", 1)
    bucket = parts[0]
    prefix = ""
    if len(parts) == 2:
        prefix = parts[1].rstrip("/")
    return bucket, prefix


def list_gs_objects(bucket_name: str, prefix: str | None) -> List[Tuple[str, str]]:
    """
    Return list of (blob_name, content_type) under the given prefix.
    If prefix is None or empty, list the whole bucket.
    """
    list_prefix = prefix if prefix else None
    blobs = _storage_client.list_blobs(bucket_name, prefix=list_prefix)
    results: List[Tuple[str, str]] = []
    for b in blobs:
        # skip directory placeholders
        if b.name.endswith("/"):
            continue
        results.append((b.name, b.content_type or ""))
    results.sort()
    return results


def looks_like_image(filename: str, content_type: str | None = None) -> bool:
    """
    Heuristic to decide if an object is an image:
    - True if content_type starts with 'image/'
    - Otherwise True when file extension is a known image extension
    """
    if content_type and content_type.startswith("image/"):
        return True
    ext = os.path.splitext(filename)[1].lower()
    return ext in _IMAGE_EXTS


def infer_format_from_ext(filename: str) -> str:
    ext = os.path.splitext(filename)[1].lower()
    mapping = {
        ".jpg": "JPEG",
        ".jpeg": "JPEG",
        ".png": "PNG",
        ".gif": "GIF",
        ".bmp": "BMP",
        ".tiff": "TIFF",
        ".tif": "TIFF",
        ".webp": "WEBP",
    }
    return mapping.get(ext, "PNG")


def create_thumbnail_bytes(image_bytes: bytes, out_format: str) -> bytes:
    """
    Create a thumbnail (max 100x100, preserving aspect) from image_bytes and
    return encoded bytes in out_format.
    """
    with Image.open(io.BytesIO(image_bytes)) as img:
        # Normalize modes
        if img.mode in ("P", "LA"):
            img = img.convert("RGBA")
        elif img.mode == "CMYK":
            img = img.convert("RGB")

        img.thumbnail((100, 100), Image.LANCZOS)

        buf = io.BytesIO()
        fmt = out_format.upper()
        # If saving JPEG and image has alpha, composite on white
        if fmt in ("JPEG", "JPG") and img.mode in ("RGBA", "LA"):
            bg = Image.new("RGB", img.size, (255, 255, 255))
            alpha = img.split()[-1]
            bg.paste(img, mask=alpha)
            bg.save(buf, format="JPEG")
        else:
            # Ensure a safe mode for formats that don't accept alpha
            if fmt not in ("PNG", "WEBP", "GIF") and img.mode not in ("RGB", "L"):
                img = img.convert("RGB")
            img.save(buf, format=fmt)
        return buf.getvalue()


def upload_bytes_to_gs(
    bucket_name: str, dest_blob_name: str, data: bytes, content_type: str | None = None
) -> None:
    bucket = _storage_client.bucket(bucket_name)
    blob = bucket.blob(dest_blob_name)
    blob.upload_from_string(data, content_type=content_type)


def process() -> None:
    # Validate INPUT_FOLDER
    if not INPUT_FOLDER:
        print(
            "ERROR: INPUT_FOLDER environment variable is required and must be a gs:// path.",
            file=sys.stderr,
        )
        sys.exit(2)
    if not is_gs_path(INPUT_FOLDER):
        print(
            "ERROR: INPUT_FOLDER must be a gs:// path. Local directories are not supported.",
            file=sys.stderr,
        )
        sys.exit(2)

    # Validate task configuration
    if TASK_COUNT <= 0:
        print("ERROR: CLOUD_RUN_TASK_COUNT must be >= 1", file=sys.stderr)
        sys.exit(2)
    if TASK_INDEX < 0 or TASK_INDEX >= TASK_COUNT:
        print(
            f"ERROR: CLOUD_RUN_TASK_INDEX {TASK_INDEX} out of range for TASK_COUNT {TASK_COUNT}",
            file=sys.stderr,
        )
        sys.exit(2)

    print(
        f"Task {TASK_INDEX + 1}/{TASK_COUNT}: Starting. INPUT_FOLDER={INPUT_FOLDER}",
        flush=True,
    )

    bucket_name, prefix = parse_gs_path(INPUT_FOLDER)
    normalized_prefix = prefix.rstrip("/") if prefix else ""
    print(f"Listing objects in gs://{bucket_name}/{normalized_prefix}", flush=True)

    objects = list_gs_objects(bucket_name, normalized_prefix)
    images: List[Tuple[str, str]] = []
    for blob_name, content_type in objects:
        fname = os.path.basename(blob_name)
        if looks_like_image(fname, content_type):
            images.append((blob_name, fname))

    total = len(images)
    if total == 0:
        print(
            f"Task {TASK_INDEX}: No images found under {INPUT_FOLDER}. Nothing to do.",
            flush=True,
        )
        return

    # Determine chunking across TASK_COUNT
    chunk_size = math.ceil(total / TASK_COUNT)
    chunk_start = chunk_size * TASK_INDEX
    chunk_end = min(chunk_start + chunk_size, total)
    if chunk_start >= total:
        print(
            f"Task {TASK_INDEX}: No indices assigned (chunk_start={chunk_start} >= total={total}). Nothing to do.",
            flush=True,
        )
        return

    assigned = images[chunk_start:chunk_end]
    print(
        f"Task {TASK_INDEX}: Found {total} images. Assigned indices [{chunk_start}, {chunk_end}) => {len(assigned)} images for this task.",
        flush=True,
    )

    # Timestamp for output folder at root of bucket, include minutes but no seconds
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%HZ")
    processed = 0
    errors = 0

    for blob_name, filename in assigned:
        try:
            bucket = _storage_client.bucket(bucket_name)
            src_blob = bucket.blob(blob_name)
            image_bytes = src_blob.download_as_bytes()

            # Try to create thumbnail in inferred format; fall back to PNG
            out_format = infer_format_from_ext(filename)
            try:
                thumb_bytes = create_thumbnail_bytes(image_bytes, out_format)
            except Exception:
                # Validate it's an image and retry as PNG
                with Image.open(io.BytesIO(image_bytes)):
                    pass
                out_format = "PNG"
                thumb_bytes = create_thumbnail_bytes(image_bytes, out_format)

            # Destination: root of bucket under timestamp folder
            dest_blob_name = f"{timestamp}/{filename}"

            content_type = (
                mimetypes.guess_type(filename)[0] or f"image/{out_format.lower()}"
            )
            upload_bytes_to_gs(
                bucket_name, dest_blob_name, thumb_bytes, content_type=content_type
            )

            print(
                f"Task {TASK_INDEX}: Processed {filename} -> gs://{bucket_name}/{dest_blob_name}",
                flush=True,
            )
            processed += 1
        except Exception as exc:
            errors += 1
            print(
                f"Task {TASK_INDEX}: Error processing gs://{bucket_name}/{blob_name}: {exc}",
                file=sys.stderr,
                flush=True,
            )

    print(
        f"Task {TASK_INDEX}: Completed. Processed {processed} image(s), {errors} error(s). Assigned indices [{chunk_start}, {chunk_end}) of {total}.",
        flush=True,
    )


if __name__ == "__main__":
    process()
