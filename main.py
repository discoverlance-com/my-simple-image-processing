"""
HTTP entrypoint that processes a single image from a Cloud Storage event,
creates a 100x100 thumbnail (preserving aspect ratio), and uploads the
thumbnail to a timestamped folder at the root of the same bucket.

This file expects CloudEvents POSTs (as produced by Cloud Storage notifications).
It uses `file_name` and `bucket` fields from the event payload (the user's
existing CloudEvent shape).

It reuses helper functions from `process.py`:
- `create_thumbnail_bytes`
- `infer_format_from_ext`
- `upload_bytes_to_gs`
"""

from __future__ import annotations

import datetime
import io
import mimetypes
import os
from typing import Any

from cloudevents.http import from_http
from flask import Flask, jsonify, request
from google.cloud import storage
from PIL import Image

# Import helpers from process.py (keeps thumbnail logic consistent)
from process import (
    create_thumbnail_bytes,
    infer_format_from_ext,
    upload_bytes_to_gs,
)

app = Flask(__name__)


def _utc_timestamp_folder() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%HZ")


@app.route("/", methods=["POST"])
def index():
    """
    Handle incoming CloudEvent POST from Cloud Storage and process a single image.

    Expected CloudEvent data shape (from user's snippet):
      event_data.bucket  -> bucket name (string)
      event_data.file_name -> object name (string)

    Returns JSON with the uploaded thumbnail path on success.
    """
    try:
        # Parse CloudEvent
        event = from_http(request.headers, request.data)
        event_data: Any = event.data
    except Exception as exc:
        return jsonify({"error": "invalid cloudevent", "details": str(exc)}), 400

    # Extract required fields
    bucket_name = getattr(event_data, "bucket", None)
    file_name = getattr(event_data, "name", None)
    generation = getattr(event_data, "generation", None)

    if not bucket_name or not file_name:
        return (
            jsonify(
                {
                    "error": "missing required CloudEvent fields",
                    "required": ["bucket", "file_name"],
                }
            ),
            400,
        )

    # Download the object bytes from GCS
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        src_blob = bucket.blob(file_name)
        image_bytes = src_blob.download_as_bytes()
    except Exception as exc:
        return jsonify({"error": "failed to download object", "details": str(exc)}), 500

    # Create thumbnail bytes. Try inferred format, fall back to PNG if needed.
    out_format = infer_format_from_ext(file_name)
    try:
        thumb_bytes = create_thumbnail_bytes(image_bytes, out_format)
    except Exception:
        # Validate it's actually an image first to provide clearer error messages
        try:
            with Image.open(io.BytesIO(image_bytes)):
                pass
        except Exception as exc:
            return jsonify(
                {"error": "source is not a valid image", "details": str(exc)}
            ), 400

        # Retry as PNG
        out_format = "PNG"
        try:
            thumb_bytes = create_thumbnail_bytes(image_bytes, out_format)
        except Exception as exc:
            return jsonify(
                {"error": "failed to create thumbnail", "details": str(exc)}
            ), 500

    # Destination path: root of bucket under timestamp folder
    timestamp = _utc_timestamp_folder()
    dest_basename = os.path.basename(file_name)
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
        return jsonify(
            {"error": "failed to upload thumbnail", "details": str(exc)}
        ), 500

    uploaded_path = f"gs://{bucket_name}/{dest_blob_name}"
    return jsonify({"status": "ok", "uploaded_to": uploaded_path}), 200


if __name__ == "__main__":
    # Respect the PORT env var that Cloud Run sets
    port = int(os.environ.get("PORT", "8080"))
    # In production (Cloud Run) you may want debug=False
    app.run(host="0.0.0.0", port=port, debug=True)
