# My Simple Image Processing

A small Python web + event service that does two related things:

1. Exposes a simple web homepage (`GET /`) with a file upload form. Uploaded images are validated server-side and uploaded to a configured Cloud Storage bucket in a timestamped folder.
2. Exposes a CloudEvent receiver (`POST /event`) intended for Cloud Storage notifications. When a notification is received, the service downloads the object, creates a 100×100 thumbnail (preserving aspect ratio) and uploads the thumbnail into a timestamped folder in the same bucket.

This repo contains:
- `process.py` — image thumbnail creation and helper functions (Pillow + google-cloud-storage).
- `main.py` — Flask web app: homepage + `/upload` + `/event` handler (idempotency, uploads).
- `requirements.txt` — Python dependencies.

Design notes
- Thumbnail creation is performed with Pillow. The thumbnail code attempts to preserve original format when possible, otherwise falls back to PNG.
- Timestamped output uses UTC hour precision in the folder name: `YYYYMMDDT%HZ` (e.g. `20251203T15Z`).
- The `/event` handler implements in-memory idempotency for Cloud Storage notifications using the key:
  ```bash
  <bucket>/<object_name>@<generation>
  ```
  The in-memory store prevents duplicate processing within the same process but is not shared between different instances or after a restart.

Important limitations
- Idempotency is in-memory only: it works only for the lifetime of a single process. If your service scales to multiple instances (Cloud Run) or restarts, duplicate processing across instances is possible. For global idempotency use a shared datastore (Cloud Storage preconditions, Firestore, Redis, etc.).
- The service stores uploaded file bytes in memory during processing. For large files or heavy concurrency, increase instance memory or stream to disk.
- The homepage relies exclusively on the `UPLOAD_BUCKET` environment variable. No bucket name is accepted from the user form.

Environment variables
- `UPLOAD_BUCKET` (required for web upload): the bucket where form-uploaded files are stored. If not set the homepage will show an error and the upload form will not be rendered.
- `FLASK_SECRET` (optional): secret key used for Flask's flash messages. Set to a secure value in production.
- `PORT` (optional): port the Flask app listens on. Cloud Run sets this automatically; default is `8080`.
- `GOOGLE_APPLICATION_CREDENTIALS` (optional for local testing): path in the container to a service account key file when testing locally. On Cloud Run, use the service account attached to the service.
- For the old job-style processing in `process.py`, the script expects `INPUT_FOLDER` and optional `CLOUD_RUN_TASK_INDEX` / `CLOUD_RUN_TASK_COUNT` — this repo now also supports the web and CloudEvent mode via `main.py`.

Endpoints
- `GET /` — homepage with upload form (only displayed when `UPLOAD_BUCKET` is set).
- `POST /upload` — form handler (multipart/form-data):
  - Validates the uploaded file with Pillow.
  - Sanitizes filename via `werkzeug.utils.secure_filename`.
  - Uploads original bytes to: `gs://<UPLOAD_BUCKET>/<TIMESTAMP>/<filename>`.
  - Flashes a success or error message and redirects to `/`.
- `POST /event` — CloudEvent receiver (expects CloudEvents produced by Cloud Storage notifications). It:
  - Extracts `bucket`, `name` or `file_name`, and `generation` from event data.
  - Uses idempotency key `<bucket>/<object_name>@<generation>` to avoid duplicate processing within the same process.
  - Downloads object, creates thumbnail (100×100), uploads thumbnail to `gs://<bucket>/<TIMESTAMP>/<basename>`.
  - Returns JSON with status and uploaded path.

Local development and testing

1. Install dependencies
   - Create a virtualenv and install:
     ```bash
     pip install -r requirements.txt
     ```
   - Required packages include:
     - `Flask`
     - `google-cloud-storage`
     - `Pillow`
     - `cloudevents` (for parsing CloudEvent payloads)

2. Run locally (no GCS access)
   - To exercise the homepage and upload behavior (without GCS access), set `UPLOAD_BUCKET` to a test bucket name and mock the upload function or run with valid credentials (see below).
   - Start the app:
     ```bash
     FLASK_APP=main.py FLASK_ENV=development python main.py
     ```
   - Visit `http://localhost:8080/`.

3. Run locally with GCS access (recommended for actual upload testing)
   - Provide credentials and set `UPLOAD_BUCKET`:
     - Place a service account key locally and point `GOOGLE_APPLICATION_CREDENTIALS` to it, or rely on Application Default Credentials:
     ```bash
     export UPLOAD_BUCKET=my-bucket
     export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
     python main.py
     ```
   - Upload via browser to test web upload.
   - The server will upload the original bytes to `gs://my-bucket/<TIMESTAMP>/<filename>`.
   
Docker & containerization
- The repository contains a Dockerfile (or you can create one similar to):
  ```dockerfile
  FROM python:3.13-slim
  WORKDIR /app
  COPY requirements.txt .
  RUN pip install --no-cache-dir -r requirements.txt
  COPY . /app
  ENTRYPOINT ["python", "main.py"]
  ```
- Build and push image:
  ```bash
  docker build -t gcr.io/YOUR_PROJECT/my-image-app:latest .
  docker push gcr.io/YOUR_PROJECT/my-image-app:latest
  ```

Deploy to Cloud Run (service)
- Use Cloud Run service to host the web + event receiver.
- Recommended steps:
  1. Ensure the service account used by Cloud Run has the following permissions on the bucket(s):
     - `roles/storage.objectViewer` (download source object)
     - `roles/storage.objectCreator` (upload thumbnail)
     - `roles/eventarc.eventReceiver` (receive eventarc events)
  2. Deploy:
     ```bash
     gcloud run deploy my-image-service \
       --image gcr.io/YOUR_PROJECT/my-image-app:latest \
       --platform managed \
       --region us-central1 \
       --set-env-vars UPLOAD_BUCKET=my-bucket,FLASK_SECRET='replace-with-secure' \
       --allow-unauthenticated
     ```
     - `--allow-unauthenticated` may be removed if you want to restrict access and require authentication.
- Configure Eventarc to send events to your service.

Security and permissions
- The service uses the Cloud Run service account for authentication when accessing GCS. Grant least privilege:
  - `storage.objects.get` and `storage.objects.create` on relevant buckets.
- If you expose the web endpoint publicly, consider authentication, rate limiting, and file-size limits.

Observability and troubleshooting
- Logs: Cloud Run writes stdout/stderr to Cloud Logging. Add additional logging where helpful.
- Common errors:
  - Missing `UPLOAD_BUCKET`: homepage shows a clear error and form is not rendered.
  - Permission denied from Cloud Run: ensure the service account has the required storage permissions.
  - Duplicate thumbnail uploads: due to in-memory idempotency, duplicates may occur across instances — use a shared idempotency store for global dedupe.

Extending this project
- Replace in-memory idempotency with a shared store (Firestore, Redis, Cloud Storage object with preconditions) to make idempotency reliable across instances.
- Add thumbnail metadata (source path, original generation) to thumbnails (object metadata).
- Introduce streaming processing for very large images to reduce memory pressure.
- Add unit and integration tests for the web and event handlers.
