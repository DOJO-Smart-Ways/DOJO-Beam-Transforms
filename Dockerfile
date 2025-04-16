# Stage 1: Base image
FROM python:3.10-slim as base

# Install Apache Beam SDK and other dependencies
RUN apt-get update && apt-get install -y git
RUN pip install --no-cache-dir \
    #dojo-beam-transforms
    git+https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms.git@release-v3.0.0

# Copy files from official SDK image, including script/dependencies.
COPY --from=apache/beam_python3.10_sdk:2.64.0 /opt/apache/beam /opt/apache/beam

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]
