# My Simple Image Processing

A small GCS-only image processing utility that:
- Lists image objects under a GCS prefix (provided via `INPUT_FOLDER`).
- Splits work across tasks using `CLOUD_RUN_TASK_INDEX` / `CLOUD_RUN_TASK_COUNT`.
- Creates 100×100 thumbnails (preserving aspect ratio) using Pillow.
- Uploads thumbnails into a timestamped folder at the root of the same bucket:
  `gs://<bucket>/<TIMESTAMP>/<original_filename>`.

This repository contains:
- `process.py` — the main script run inside the container.
- `requirements.txt` — Python dependencies (`google-cloud-storage`, `Pillow`).
- `Procfile` — present for buildpack compatibility (not required if you use a Dockerfile).

Overview / design notes
- Input is strictly cloud storage. `INPUT_FOLDER` must be a `gs://` path (e.g. `gs://my-bucket/some/prefix`).
- The script identifies images using blob `content-type` (if present) or common filename extensions.
- Chunking (partitioning) is computed from the total number of discovered images and split across `TASK_COUNT` and `TASK_INDEX`.
- Output thumbnails are uploaded at the bucket root under a timestamp folder (minute precision), for example:
  `gs://my-bucket/20251203T1530Z/file.jpg`.
- The timestamp uses hour precision (format `YYYYMMDDTHHZ`) to group thumbnails into hour-level folders.

Local testing with Docker
1. Inspect the image entrypoint and files:
```bash
docker inspect --format '{{.Config.Entrypoint}} {{.Config.Cmd}}' IMAGE_URI
```

List contents of the image:
```bash
docker run --rm --entrypoint ls IMAGE_URI -la /app
```

2. Run the image using the image's default entrypoint (simple test):
```bash
docker run --rm \
  -e INPUT_FOLDER=gs://my-bucket/path \
  -e PYTHONUNBUFFERED=1 \
  IMAGE_URI
```

3. Force-run `process.py` regardless of image entrypoint (recommended for debug):
```bash
docker run --rm \
  -e INPUT_FOLDER=gs://my-bucket/path \
  -e PYTHONUNBUFFERED=1 \
  --entrypoint python \
  IMAGE_URI process.py
```

4. Run with Google credentials (if you need GCS access from local):
```bash
docker run --rm \
  -e INPUT_FOLDER=gs://my-bucket/path \
  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/key.json \
  -e PYTHONUNBUFFERED=1 \
  -v /path/on/host/key.json:/secrets/key.json:ro \
  --entrypoint python \
  IMAGE_URI process.py
```

Build and push container image (recommended: Docker + Artifact Registry)
Example Dockerfile (create `Dockerfile` in repo root):
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
ENTRYPOINT ["python", "process.py"]
```

Build & push example:
```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
REGION=us-central1
REPOSITORY=your-repo
IMAGE=your-image
TAG=latest
IMAGE_URI=LOCATION-docker.pkg.dev/YOUR_PROJECT_ID/$REPOSITORY/$IMAGE:$TAG

docker build -t $IMAGE_URI .
docker push $IMAGE_URI
```

Create and run a Cloud Run Job
Create the job (example):
```bash
gcloud run jobs create my-image-job \
  --image $IMAGE_URI \
  --region $REGION \
  --command python --args process.py
```
If your image already sets ENTRYPOINT to `python process.py`, you can omit `--command`.

Configure env vars (must set INPUT_FOLDER):
```bash
gcloud run jobs update my-image-job \
  --set-env-vars INPUT_FOLDER=gs://my-bucket/path,CLOUD_RUN_TASK_COUNT=4 \
  --region $REGION
```

Run the job:
```bash
gcloud run jobs run my-image-job --region $REGION
```

Notes on parallelism (chunking)
- The script partitions discovered images across `TASK_COUNT`. Each worker should be invoked with a distinct `CLOUD_RUN_TASK_INDEX` in `[0..TASK_COUNT-1]`.
- Cloud Run Jobs supports task-level parallelism; ensure your orchestrator or job configuration supplies the correct indices to workers.

Output layout and naming collisions
- Thumbnails are uploaded to the bucket root under a timestamp folder:
  `gs://<bucket>/<TIMESTAMP>/<original_filename>`
- Timestamp uses minute precision: `YYYYMMDDTHHZ` (UTC, no seconds, minutes).
- If multiple source files share the same filename and are processed in the same minute, consider preserving source subpaths or adding unique suffixes to avoid overwrites.

Troubleshooting tips
- Container exits immediately with no logs:
  - Inspect the image entrypoint/CMD with `docker inspect`.
  - Force-run Python (`--entrypoint python IMAGE_URI process.py`) to confirm the script runs.
  - Verify `process.py` exists in the image.
- Missing `INPUT_FOLDER`: the script exits early and prints an error. Ensure env var is set.
- Authentication errors: ensure the running service account has `storage.objects.get` and `storage.objects.create` permissions, or mount credentials during local testing.
- Memory/IO pressure: the script downloads each blob into memory; increase container memory or stream to disk if needed.

Extending the project
- Preserve source subpaths in the output folder (recommended for collision avoidance).
- Add an `OUTPUT_BUCKET` env var to write thumbnails to a separate bucket.
- Make image detection more robust by validating file contents (tradeoff: extra downloads).
- Add per-task concurrency or batching inside a worker, carefully balancing memory and network I/O.

Files of interest
- `process.py` — main logic (GCS listing, chunking, Pillow processing, upload).
- `requirements.txt` — required Python packages.
- `Dockerfile` — recommended to control image ENTRYPOINT.

If you want, I can:
- Export the Mermaid diagrams to PNG/SVG and add them to the repo for viewers that don't render Mermaid.
- Add a Dockerfile to the repo and a small Makefile to build/push/run the Cloud Run Job.
- Modify `process.py` to preserve sub-paths under the timestamp folder or add an `OUTPUT_BUCKET` option.
