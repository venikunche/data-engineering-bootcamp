FROM jupyter/pyspark-notebook:latest

WORKDIR /home/jovyan/work

# Install any additional Python packages you might need
RUN pip install --no-cache-dir wget

USER root

# Install wget to download data
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

USER jovyan
