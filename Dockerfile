# Stage 1: Base image
FROM python:3.11-slim as base

# Install Apache Beam SDK and other dependencies
RUN apt-get update && apt-get install -y git
RUN pip install --no-cache-dir \
    git+https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms.git@main#egg=dojo-beam-transforms


# Copy files from official SDK image, including script/dependencies.
COPY --from=apache/beam_python3.11_sdk:2.58.1 /opt/apache/beam /opt/apache/beam

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]
