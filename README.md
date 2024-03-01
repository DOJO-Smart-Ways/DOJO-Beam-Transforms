# DOJO-Beam-Transforms

Welcome to `DOJO-Beam-Transforms`, a repository dedicated to sharing advanced Apache Beam transformations, custom `DoFn` classes, and best practices for scalable data processing, curated by the team at DOJO-Smart-Ways.

## About DOJO-Smart-Ways

DOJO-Smart-Ways is committed to advancing data engineering, providing solutions that enhance data processing capabilities, and sharing knowledge within the data engineering community. Our focus is on creating efficient, scalable solutions for real-world data challenges.

## What You'll Find Here

This repository contains:

- **Custom Apache Beam Transformations**: Reusable code snippets for specific data preparation tasks.
- **Data Processing Recipes**: Step-by-step guides for common and advanced data processing scenarios.
- **Integration Examples**: How to integrate Apache Beam pipelines with BigQuery and other cloud services for end-to-end data processing solutions.
- **Performance Optimization Tips**: Best practices for optimizing your Apache Beam pipelines for performance and cost.

## Quick Start Guide

**Kick off your journey with `DOJO-Beam-Transforms` effortlessly!** Follow these simple steps:

1. **Clone the Repository:**

   Dive into the project by cloning the repository with the command below:
   ```bash
   git clone https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms.git
   ```

2. **Set Up Dependencies:**

   Incorporate the project into your environment. Add this line to your `requirements.txt`:
   ```
   git+https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms.git#egg=dojo-beam-transforms
   ```

3. **Utilize the Components:**

   Bring the power of `DOJO-Beam-Transforms` into your pipeline with ease:
   ```python
   from pipeline_components.input_file import read_pdf, read_and_apply_headers, read_bq
   from pipeline_components import data_enrichment as de
   from pipeline_components import data_cleaning as dc
   ```

## Pipeline Deployment with Docker image

## Prerequisites

- **Docker installed on your machine.**
- **Google Cloud SDK installed..**

1. **Clone the Dockerfile**

2. **Build the Docker Image**
   Inside the folder where Dockerfile is located run: `docker build -t image_name .`

3. **Authenticate with Google Cloud**
   Configure Docker to authenticate requests for Artifact Registry using the following command: `gcloud auth configure-docker [REGION]-docker.pkg.dev`

5. **Tag your Docker image**
   Use the following command: `docker tag image_name [REGION]-docker.pkg.dev/[PROJECT_ID]/[REPOSITORY]/image_name`

6. **Push the Docker image to Artifact Registry**
   Use the following command: `docker push [REGION]-docker.pkg.dev/[PROJECT_ID]/[REPOSITORY]/image_name`

7. **Run the Dataflow Pipeline with Custom Container**
  Add these two parameters to yout pipeline options
      pipeline_options = {
       'sdk_container_image': 'us-central1-docker.pkg.dev/nidec-ga/dojo-beam/dojo_beam',
       'sdk_location': 'container'}

**Embark on your data processing journey with `DOJO-Beam-Transforms` today!**
