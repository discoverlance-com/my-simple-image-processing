from __future__ import annotations

import datetime
import io
import mimetypes
import os
import threading
from typing import Any, Dict, Optional

from cloudevents.http import from_http
from flask import (
    Flask,
    flash,
    get_flashed_messages,
    jsonify,
    redirect,
    render_template_string,
    request,
    url_for,
)
from google.cloud import storage
from PIL import Image
from werkzeug.utils import secure_filename

# Reuse helpers from process.py so thumbnail behavior matches
from process import create_thumbnail_bytes, infer_format_from_ext, upload_bytes_to_gs

app = Flask(__name__)
# Flashing requires a secret key; override in production via FLASK_SECRET env var.
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret")

# In-memory idempotency store (per-process)
# key -> { "status": "processing"|"completed"|"failed", "dest_blob": str, "uploaded_at": str, "error": str }
_idempotency_store: Dict[str, Dict[str, Any]] = {}
_idempotency_lock = threading.Lock()


def _utc_timestamp_folder() -> str:
    """Return UTC timestamp folder like YYYYMMDDT%HZ (hour precision)."""
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%HZ")


def _extract_field(data: Any, *keys: str) -> Optional[Any]:
    """
    Safely extract a value from CloudEvent data which may be a mapping or an object.
    Tries each key in order and returns the first non-None value.
    """
    for key in keys:
        try:
            if isinstance(data, dict) and key in data:
                return data[key]
        except Exception:
            pass
        try:
            val = getattr(data, key, None)
            if val is not None:
                return val
        except Exception:
            pass
    return None


# ---------------- Homepage ----------------
HOME_HTML = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Upload image</title>
    <style>
      body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial; margin: 24px; }
      .error { color: darkred; font-weight: bold; }
      .hint { color: #444; }
      #flashes { margin-top: 1rem; color: green; }
    </style>
  </head>
  <body>
    <h1>Upload an image</h1>

    {% if not upload_bucket %}
      <p class="error">Error: Server is not configured with an upload bucket. Set the UPLOAD_BUCKET environment variable.</p>
      <p class="hint">The upload form is not available until UPLOAD_BUCKET is set.</p>
    {% else %}
      <p>Destination bucket: <strong>{{ upload_bucket }}</strong></p>

      {% if messages %}
        <ul id="flashes">
        {% for msg in messages %}
          <li>{{ msg }}</li>
        {% endfor %}
        </ul>
      {% endif %}

      <form method="POST" action="{{ url_for('upload') }}" enctype="multipart/form-data">
        <label for="file">Select image file:</label><br/>
        <input id="file" name="file" type="file" accept="image/*" required><br/><br/>
        <button type="submit">Upload</button>
      </form>
    {% endif %}
  </body>
</html>
"""


@app.route("/", methods=["GET"])
def home():
    """
    Render the homepage. No bucket input is provided — the app depends on the
    UPLOAD_BUCKET environment variable. If unset, the page shows an error and
    the upload form is not present.
    """
    upload_bucket = os.environ.get("UPLOAD_BUCKET", "")
    messages = get_flashed_messages()
    return render_template_string(
        HOME_HTML, upload_bucket=upload_bucket, messages=messages
    )


# ---------------- Upload handler ----------------
@app.route("/upload", methods=["POST"])
def upload():
    """
    Handle form uploads. Uses only the configured UPLOAD_BUCKET.
    Server-side validation:
    - Ensure UPLOAD_BUCKET is configured.
    - Ensure a file was uploaded.
    - Validate file is an image via Pillow.
    - Sanitize filename using secure_filename.
    - Upload to gs://<UPLOAD_BUCKET>/<TIMESTAMP>/<filename>
    """
    bucket_name = os.environ.get("UPLOAD_BUCKET")
    if not bucket_name:
        flash("Server is not configured with UPLOAD_BUCKET. Contact the administrator.")
        return redirect(url_for("home"))

    uploaded_file = request.files.get("file")
    if not uploaded_file or not uploaded_file.filename:
        flash("No file selected.")
        return redirect(url_for("home"))

    # secure_filename expects a str; coerce if necessary
    raw_filename: str = str(uploaded_file.filename or "")
    filename = secure_filename(raw_filename)
    if not filename:
        flash("Invalid filename after sanitization.")
        return redirect(url_for("home"))

    try:
        file_bytes = uploaded_file.read()
    except Exception as exc:
        flash(f"Failed to read uploaded file: {exc}")
        return redirect(url_for("home"))

    # Validate with Pillow
    try:
        with Image.open(io.BytesIO(file_bytes)):
            pass
    except Exception as exc:
        flash(f"Uploaded file is not a valid image: {exc}")
        return redirect(url_for("home"))

    timestamp = _utc_timestamp_folder()
    dest_blob_name = f"{timestamp}/{filename}"

    content_type = (
        uploaded_file.mimetype
        or mimetypes.guess_type(filename)[0]
        or f"image/{infer_format_from_ext(filename).lower()}"
    )

    try:
        upload_bytes_to_gs(
            bucket_name, dest_blob_name, file_bytes, content_type=content_type
        )
    except Exception as exc:
        flash(f"Failed to upload to gs://{bucket_name}/{dest_blob_name}: {exc}")
        return redirect(url_for("home"))

    flash(f"Successfully uploaded to gs://{bucket_name}/{dest_blob_name}")
    return redirect(url_for("home"))


# ---------------- CloudEvent handler (/event) ----------------
@app.route("/event", methods=["POST"])
def handle_event():
    """
    CloudEvent handler for Cloud Storage notifications.
    Expects event.data to contain 'bucket', 'name' or 'file_name', and 'generation'.
    Idempotency key: "<bucket>/<object_name>@<generation>".
    """
    try:
        event = from_http(request.headers, request.get_data())
        event_data: Any = event.data
    except Exception as exc:
        return jsonify({"error": "invalid cloudevent", "details": str(exc)}), 400

    bucket_name = _extract_field(event_data, "bucket")
    object_name = _extract_field(event_data, "file_name", "name")
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

    generation_str = str(generation)
    idempotency_key = f"{bucket_name}/{object_name}@{generation_str}"

    # Idempotency check and transition to processing
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
            if status == "processing":
                return (
                    jsonify(
                        {
                            "status": "processing",
                            "idempotency_key": idempotency_key,
                            "message": "Processing already in progress for this key",
                        }
                    ),
                    409,
                )
            if status == "failed":
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

        # Mark as processing so another thread in this process won't duplicate work
        _idempotency_store[idempotency_key] = {"status": "processing"}

    # Download object bytes
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        src_blob = bucket.blob(object_name)
        image_bytes = src_blob.download_as_bytes()
    except Exception as exc:
        with _idempotency_lock:
            _idempotency_store[idempotency_key] = {
                "status": "failed",
                "error": f"download_error: {exc}",
            }
        return jsonify({"error": "failed to download object", "details": str(exc)}), 500

    # Create thumbnail (try inferred format, fall back to PNG)
    out_format = infer_format_from_ext(object_name)
    try:
        thumb_bytes = create_thumbnail_bytes(image_bytes, out_format)
    except Exception:
        # Verify it's an image to provide clearer error
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

    timestamp = _utc_timestamp_folder()
    dest_basename = object_name.split("/")[-1]
    dest_blob_name = f"{timestamp}/resized/{dest_basename}"

    content_type = (
        mimetypes.guess_type(dest_basename)[0] or f"image/{out_format.lower()}"
    )

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
    # Respect the PORT env var that Cloud Run or other hosts may set.
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
