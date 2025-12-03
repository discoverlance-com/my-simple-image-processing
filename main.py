#!/usr/bin/env python3
"""
HTTP entrypoint that processes a single image from a Cloud Storage event,
creates a 100x100 thumbnail (preserving aspect ratio), and uploads the
thumbnail to a timestamped folder at the root of the same bucket.

This file expects CloudEvents POSTs (as produced by Cloud Storage notifications).
It uses `name` or `file_name`, `bucket` and `generation` fields from the event payload.

Idempotency:
- This implementation uses an in-memory global "database" (a dict) to track
  operations. The idempotency key is constructed as:
      <bucket>/<object_name>@<generation>
- When a request arrives for a key already marked "completed", the handler
  returns the previous result (no reprocessing).
- When a request arrives for a key currently marked "processing", the handler
  returns a 409 to indicate the work is in progress.
- The in-memory store only persists for the lifetime of the process and is
  not shared across instances

Note: This file reuses thumbnail logic from process.py to keep behavior consistent.
"""

from __future__ import annotations

import datetime
import io
import mimetypes
import os
import threading
from typing import Any, Dict, Optional

from cloudevents.http import from_http
from flask import Flask, jsonify, request
from google.cloud import storage
from PIL import Image

from process import create_thumbnail_bytes, infer_format_from_ext, upload_bytes_to_gs

app = Flask(__name__)

# In-memory idempotency store:
# key -> {
#    "status": "processing" | "completed" | "failed",
#    "dest_blob": "gs://bucket/timestamp/name" (when completed),
#    "uploaded_at": ISO timestamp (when completed),
#    "error": error string (when failed)
# }
_idempotency_store: Dict[str, Dict[str, Any]] = {}
_idempotency_lock = threading.Lock()


def _utc_timestamp_folder() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%HZ")


def _extract_field(data: Any, *keys: str) -> Optional[Any]:
    """
    Helper to extract a field from the CloudEvent data. The event data may be
    an object with attributes or a dict-like mapping.
    Tries each key in order and returns the first found value or None.
    """
    for key in keys:
        # If mapping-like
        try:
            if isinstance(data, dict) and key in data:
                return data[key]
        except Exception:
            pass
        # Try attribute access
        try:
            val = getattr(data, key, None)
            if val is not None:
                return val
        except Exception:
            pass
    return None


@app.route("/", methods=["POST"])
def index():
    """
    Handle incoming CloudEvent POST from Cloud Storage and process a single image.

    Required fields in CloudEvent data:
      - bucket
      - name or file_name
      - generation

    Idempotency key: "<bucket>/<name>@<generation>"
    """
    # Parse CloudEvent
    try:
        event = from_http(request.headers, request.get_data())
        event_data: Any = event.data
    except Exception as exc:
        return jsonify({"error": "invalid cloudevent", "details": str(exc)}), 400

    # Extract required fields (support both dict and object shapes)
    bucket_name = _extract_field(event_data, "bucket")
    object_name = _extract_field(event_data, "name")
    generation = _extract_field(event_data, "generation")

    if not bucket_name or not object_name or generation is None:
        return (
            jsonify(
                {
                    "error": "missing required CloudEvent fields",
                    "required": ["bucket", "file_name or name", "generation"],
                }
            ),
            400,
        )

    # Normalize generation to string for stable idempotency key
    generation_str = str(generation)
    idempotency_key = f"{bucket_name}/{object_name}@{generation_str}"

    # Check idempotency store
    with _idempotency_lock:
        existing = _idempotency_store.get(idempotency_key)
        if existing:
            status = existing.get("status")
            if status == "completed":
                return (
                    jsonify(
                        {
                            "status": "already_processed",
                            "idempotency_key": idempotency_key,
                            "uploaded_to": existing.get("dest_blob"),
                            "uploaded_at": existing.get("uploaded_at"),
                        }
                    ),
                    200,
                )
            elif status == "processing":
                return (
                    jsonify(
                        {
                            "status": "processing",
                            "idempotency_key": idempotency_key,
                            "message": "Processing is already in progress for this key",
                        }
                    ),
                    409,
                )
            elif status == "failed":
                return (
                    jsonify(
                        {
                            "status": "failed",
                            "idempotency_key": idempotency_key,
                            "error": existing.get("error"),
                        }
                    ),
                    500,
                )

        # Mark the key as processing before releasing lock to avoid races
        _idempotency_store[idempotency_key] = {"status": "processing"}

    # Proceed to download the object and create thumbnail
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        src_blob = bucket.blob(object_name)
        image_bytes = src_blob.download_as_bytes()
    except Exception as exc:
        # Record failure in idempotency store
        with _idempotency_lock:
            _idempotency_store[idempotency_key] = {
                "status": "failed",
                "error": f"download_error: {exc}",
            }
        return jsonify({"error": "failed to download object", "details": str(exc)}), 500

    # Create thumbnail bytes. Try inferred format, fall back to PNG if needed.
    out_format = infer_format_from_ext(object_name)
    try:
        thumb_bytes = create_thumbnail_bytes(image_bytes, out_format)
    except Exception:
        # Validate it's actually an image first
        try:
            with Image.open(io.BytesIO(image_bytes)):
                pass
        except Exception as exc:
            with _idempotency_lock:
                _idempotency_store[idempotency_key] = {
                    "status": "failed",
                    "error": f"invalid_image: {exc}",
                }
            return jsonify(
                {"error": "source is not a valid image", "details": str(exc)}
            ), 400

        # Retry as PNG
        out_format = "PNG"
        try:
            thumb_bytes = create_thumbnail_bytes(image_bytes, out_format)
        except Exception as exc:
            with _idempotency_lock:
                _idempotency_store[idempotency_key] = {
                    "status": "failed",
                    "error": f"thumbnail_error: {exc}",
                }
            return jsonify(
                {"error": "failed to create thumbnail", "details": str(exc)}
            ), 500

    # Destination path: root of bucket under timestamp folder
    timestamp = _utc_timestamp_folder()
    dest_basename = object_name.split("/")[-1]
    dest_blob_name = f"{timestamp}/{dest_basename}"

    # Determine content type for uploaded thumbnail
    content_type = (
        mimetypes.guess_type(dest_basename)[0] or f"image/{out_format.lower()}"
    )

    # Upload thumbnail to GCS
    try:
        upload_bytes_to_gs(
            bucket_name, dest_blob_name, thumb_bytes, content_type=content_type
        )
    except Exception as exc:
        with _idempotency_lock:
            _idempotency_store[idempotency_key] = {
                "status": "failed",
                "error": f"upload_error: {exc}",
            }
        return jsonify(
            {"error": "failed to upload thumbnail", "details": str(exc)}
        ), 500

    uploaded_path = f"gs://{bucket_name}/{dest_blob_name}"
    uploaded_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Record completion in idempotency store
    with _idempotency_lock:
        _idempotency_store[idempotency_key] = {
            "status": "completed",
            "dest_blob": uploaded_path,
            "uploaded_at": uploaded_at,
        }

    return jsonify(
        {
            "status": "ok",
            "idempotency_key": idempotency_key,
            "uploaded_to": uploaded_path,
        }
    ), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
