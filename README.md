# DOJO-Beam-Transforms

Welcome to `DOJO-Beam-Transforms`, a repository dedicated to sharing advanced Apache Beam transformations, custom `DoFn` classes, and best practices for scalable data processing, curated by the team at DOJO-Smart-Ways.


## Table of Contents

1. [DOJO-Beam-Transforms](#dojo-beam-transforms)
2. [About DOJO-Smart-Ways](#about-dojo-smart-ways)
3. [What You'll Find Here](#what-youll-find-here)
4. [Dependency Versions for Release 1.0.0](#dependency-versions-for-release-100)
5. [Quick Start Guide](#quick-start-guide)
6. [Pipeline Deployment with Docker image](#pipeline-deployment-with-docker-image)

## About DOJO-Smart-Ways

DOJO-Smart-Ways is committed to advancing data engineering, providing solutions that enhance data processing capabilities, and sharing knowledge within the data engineering community. Our focus is on creating efficient, scalable solutions for real-world data challenges.

## What You'll Find Here

This repository contains:

- **Custom Apache Beam Transformations**: Reusable code snippets for specific data preparation tasks.
- **Data Processing Recipes**: Step-by-step guides for common and advanced data processing scenarios.
- **Integration Examples**: How to integrate Apache Beam pipelines with BigQuery and other cloud services for end-to-end data processing solutions.
- **Performance Optimization Tips**: Best practices for optimizing your Apache Beam pipelines for performance and cost.


## Dependency Versions for Release 1.0.0

The following is a list of the dependencies and their respective versions that are required and compatible with the `dojo-beam-transforms` package version 1.0.0:

### Apache Beam SDK Version

- `apache-beam[dataframe,gcp,interactive] == 2.58.1`

### Other Dependencies

- `pandas == 2.0.3`
- `pandas-datareader == 0.10.0`
- `PyMuPDF == 1.23.22`
- `pypinyin == 0.51.0`
- `unidecode == 1.3.8`
- `openpyxl == 3.0.10`
- `fsspec == 2024.6.1`
- `gcsfs == 2024.6.1`

### Compatible Python Versions

The following Python versions have been tested and are confirmed to be compatible with this release:

- Python 3.10
- Python 3.11

Please ensure that your environment meets these requirements for optimal performance and compatibility.


## Quick Start Guide

**Streamline Your Setup with `DOJO-Beam-Transforms`!** Begin your development journey smoothly by following this streamlined step:

1. **Initialize Your Development Environment:**
   
   Start by creating a new branch for your project within the `DOJO-Beam-Transforms` repository. This approach ensures you can develop and iterate on your generic classes tailored to the project's needs. Use the following commands to clone the repository and switch to a new branch named after your project:

   ```bash
   # Clone the DOJO-Beam-Transforms repository
   git clone https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms.git
   cd DOJO-Beam-Transforms
   
   # Create and switch to a new branch named 'project_name'
   git checkout -b project_name
   ```

   Once your project-specific development is underway, you can seamlessly integrate these changes into your Jupyter notebook environment. Execute the command below to install the project branch directly:

   ```bash
   !pip install dojo-beam-transforms
   ```

   This method allows for continuous development and testing within your project's scope, enabling a more efficient workflow.

2. **Consolidate Progress and Manage Dependencies:**

   After validating the effectiveness of your enhancements or new features, merge your working progress from the `project_name` branch into the main branch. This step is crucial for consolidating your efforts and ensuring the broader project benefits from your contributions. Additionally, if your development introduced new dependencies, remember to update the `setup.py` file accordingly to include these dependencies. This ensures anyone pulling from the main branch or installing the package gets a version with all necessary dependencies resolved.

By following this integrated approach, you maintain a clean and organized development process, facilitating collaboration and ensuring that your enhancements are systematically incorporated into the `DOJO-Beam-Transforms` project.

3. **Utilize the Components:**

   Bring the power of `DOJO-Beam-Transforms` into your pipeline with ease:
   ```python
   from pipeline_components.input_file import read_pdf, read_and_apply_headers, read_bq
   from pipeline_components import data_enrichment as de
   from pipeline_components import data_cleaning as dc
   
   def process_delivery_requests(temp_location, output_table):
   
   # Reading the initial data
   delivery_requests, invalid_delivery_requests = read_json(pipeline, 'bucket/location/file.json, identifier='')
   
   # Cleaning the data
   cleaned_data = (
      delivery_requests
      | 'Keep Only BR Currency' >> beam.ParDo(dc.KeepColumnValues('Currency', ['R$', '$']))
      | 'Replace , to .  on Coordinates' >> beam.ParDo(dc.ReplacePatterns(['Longitude', 'Latitude'], ',', '.'))
   )
   
   # Enriching the data
   enriched_data = (
      cleaned_data
      | 'Convert to String' >> beam.ParDo(de.ColumnsToStringConverter(), ['destination', 'origin'])
   )
   
   # Writing the final output to BigQuery
   enriched_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
      table=output_table,
      schema='SCHEMA_AUTODETECT',
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
      custom_gcs_temp_location=temp_location
   )
   
   # Run the pipeline
   pipeline.run().wait_until_finish()
   
   if __name__ == '__main__':
      temp_location = 'path/to/temp/location'
      output_table = 'project-id:dataset.table'
      process_delivery_requests(temp_location, output_table)
   ```

## Pipeline Deployment with Docker Image

### Benefits of Saving a Docker Image

Saving your Docker image provides several advantages, including consistency across environments, ease of deployment, and faster start-up times. By saving the image, you ensure that the exact environment used in development is replicated in production, reducing the chances of discrepancies or bugs. Additionally, storing Docker images allows for easy rollbacks to previous versions if needed, and simplifies the process of scaling deployments across multiple instances.

### Storage Options

In the example below, the Docker image is stored in **[Google Cloud's Artifact Registry](https://cloud.google.com/artifact-registry/docs/docker/store-docker-container-images)**, a managed service that allows you to securely store and manage your container images. While the Artifact Registry is a convenient option, especially for projects already using Google Cloud, Docker images can also be stored in other commonly used registries, including:

- **Docker Hub**: A popular and widely used registry for storing public and private images.
- **Amazon Elastic Container Registry (ECR)**: A service provided by AWS for managing Docker containers within the AWS ecosystem.
- **Azure Container Registry (ACR)**: A managed Docker container registry service provided by Microsoft Azure.

### Prerequisites

- **Docker installed on your machine.**
- **Google Cloud SDK installed.**

1. **Clone the Dockerfile**

2. **Build the Docker Image**
   Inside the folder where Dockerfile is located run: `docker build -t [IMAGE_NAME] .`

3. **Authenticate with Google Cloud**
   Configure Docker to authenticate requests for Artifact Registry using the following command: `gcloud auth configure-docker [REGION]-docker.pkg.dev`

5. **Tag your Docker image**
   Use the following command: `docker tag [IMAGE_NAME]:[VERSION] [REGION]-docker.pkg.dev/[PROJECT_ID]/[REPOSITORY]/[IMAGE_NAME]`

6. **Push the Docker image to Artifact Registry**
   Use the following command: `docker push [REGION]-docker.pkg.dev/[PROJECT_ID]/[REPOSITORY]/[IMAGE_NAME]`

7. **Run the Dataflow Pipeline with Custom Container**
  Add these two parameters to yout pipeline options
      pipeline_options = {
       'sdk_container_image': '[REGION]-docker.pkg.dev/[PROJECT_ID]/dojo-beam/[IMAGE_NAME]',
       'sdk_location': 'container'}

**Embark on your data processing journey with `DOJO-Beam-Transforms` today!**
