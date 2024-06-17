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
   !pip install git+https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms.git@project_name#egg=dojo-beam-transforms
   ```

   This method allows for continuous development and testing within your project's scope, enabling a more efficient workflow.

2. **Consolidate Progress and Manage Dependencies:**

   After validating the effectiveness of your enhancements or new features, merge your working progress from the `project_name` branch into the main branch. This step is crucial for consolidating your efforts and ensuring the broader project benefits from your contributions. Additionally, if your development introduced new dependencies, remember to update the `setup.py` file accordingly to include these dependencies. This ensures anyone pulling from the main branch or installing the package gets a version with all necessary dependencies resolved.

By following this integrated approach, you maintain a clean and organized development process, facilitating collaboration and ensuring that your enhancements are systematically incorporated into the `DOJO-Beam-Transforms` project.

3. **Utilize the Components:**

   Bring the power of `DOJO-Beam-Transforms` into your pipeline with ease:
   ```python
   from pipeline_components.input_file import read_json
   from pipeline_components import data_enrichment as de
   from pipeline_components import data_cleaning as dc
   
   def process_delivery_requests(temp_location, output_table):
   
   # Reading the initial data
   delivery_requests, invalid_delivery_requests = read_json(pipeline, 'bucket/location/file.json, identifier='')
   
   # Cleaning the data
   cleaned_data = (delivery_requests
      | 'Keep Only BR Currency' >> beam.ParDo(dc.KeepColumnValues('Currency', 'R$'))
      | 'Replace , to .  on Coordinates' >> beam.ParDo(dc.ReplacePatterns(), ['Longitude', 'Latitude'], ',', '.'))
   
   # Enriching the data
   enriched_data = (cleaned_data
      | 'Convert to String' >> beam.ParDo(de.ColumnsToStringConverter(), ['destination', 'origin']))
   
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
