FROM python:3.13-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app
# Ensure the script is executed as the container PID 1 process:
ENTRYPOINT ["python", "process.py"]
