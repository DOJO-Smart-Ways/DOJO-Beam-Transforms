# Stage 1: Base image
FROM python:3.13-slim AS base

# Install Apache Beam SDK and other dependencies
RUN apt-get update && apt-get install -y git
RUN pip install --no-cache-dir \
    #dojo-beam-transforms
    git+https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms.git@release-v3.1.0

# Copy files from official SDK image, including script/dependencies.
COPY --from=apache/beam_python3.13_sdk:2.71.0 /opt/apache/beam /opt/apache/beam

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]
