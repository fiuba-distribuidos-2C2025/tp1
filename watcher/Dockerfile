FROM python:3.11-slim

WORKDIR /app

# Copy the watcher script
COPY watcher/watcher.py .

# Install dependencies
RUN pip install --no-cache-dir docker requests

# Run the watcher
CMD ["python3", "watcher.py"]
