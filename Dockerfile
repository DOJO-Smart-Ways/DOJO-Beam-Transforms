# Stage 1: Base image
FROM python:3.10-slim as base

# Install Apache Beam SDK and other dependencies
RUN pip install --no-cache-dir \
    apache-beam[gcp]==2.54.0 \
    gcsfs \
    fsspec==2023.6.0 \
    pandas==1.5.3 \
    pandas-datareader==0.10.0 \
    google-cloud-bigquery==3.12.0 \
	odfpy==1.4.1

# Copy files from the official SDK image, including script/dependencies
COPY --from=apache/beam_python3.10_sdk:2.48.0 /opt/apache/beam /opt/apache/beam

# Set the entrypoint to Apache Beam SDK launcher
ENTRYPOINT ["/opt/apache/beam/boot"]