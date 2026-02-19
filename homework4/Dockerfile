# Use official Python image
FROM python:3.10-slim

# Install system dependencies and clean up
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /workspaces/docker-workshop/homework4

# Copy requirements and script
COPY load_taxi_data.py .     
COPY gcs.json .             

# Install Python dependencies
RUN pip install --no-cache-dir google-cloud-storage

# Set environment variable for Google credentials (optional, if your script needs it)
ENV GOOGLE_APPLICATION_CREDENTIALS=/workspaces/docker-workshop/homework4/gcs.json

# Command to run the script
CMD ["python", "load_taxi_data.py"]
