# DOJO-Beam-Transforms

`DOJO-Beam-Transforms` is a reusable collection of Apache Beam transforms and utility components for data cleaning, enrichment, and ingestion workflows.

## Current Release

- Package: `dojo-beam-transforms`
- Current release: `3.1.1`
- Python target: `3.12`

## Compatibility Matrix (v3.1.1)

### Apache Beam SDK

- `apache-beam[dataframe,gcp,interactive]==2.72.0`

### Core Dependencies

- `pandas==2.1.1`
- `numpy==1.26.3`
- `pytz==2025.2`
- `openpyxl==3.1.5`

## Installation

### Install from PyPI

```bash
pip install dojo-beam-transforms==3.1.1
```

### Install from GitHub tag

```bash
pip install "git+https://github.com/DOJO-Smart-Ways/DOJO-Beam-Transforms.git@release-v3.1.1"
```

## Updated Usage Examples

### Example 1: CSV input + cleaning + enrichment

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from pipeline_components.input_file import read_csvs_union
from pipeline_components import data_cleaning
from pipeline_components import data_enrichment


pipeline_options = PipelineOptions()

with beam.Pipeline(options=pipeline_options) as pipeline:
    records = read_csvs_union(
        pipeline=pipeline,
        input_pattern="gs://my-bucket/input/*.csv",
        delimiter=";",
        identifier="orders"
    )

    cleaned = (
        records
        | "Keep Relevant Columns" >> beam.ParDo(
            dc.KeepColumns(["order_id", "status", "amount"])
        )
        | "Normalize Status" >> beam.ParDo(
            dc.ReplaceValues(["status"], {"": "UNKNOWN", None: "UNKNOWN"})
        )
        | "Clean Amount Regex" >> beam.ParDo(
            dc.ReplaceRegex(["amount"], [(r",", ".")])
        )
    )

    enriched = (
        cleaned
        | "Cast Amount To Float" >> beam.ParDo(ColumnsToFloat(["amount"]))
        | "Force Order Id As String" >> beam.ParDo(ColumnsToString(["order_id"]))
    )

    _ = enriched | "Write Output" >> beam.io.WriteToText("gs://my-bucket/output/orders")
```

### Example 2: Read from BigQuery and write to BigQuery

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from pipeline_components.input_file import read_bq
from pipeline_components.data_cleaning.TrimValues import TrimValues
from pipeline_components.data_cleaning.DropDuplicates import DropDuplicates


temp_location = "gs://my-project-temp/dataflow"
query = """
SELECT order_id, customer_name, city
FROM `my_project.my_dataset.orders`
"""

pipeline_options = PipelineOptions()

with beam.Pipeline(options=pipeline_options) as pipeline:
    rows = read_bq(
        pipeline=pipeline,
        query=query,
        temp_location=temp_location,
        use_standard_sql=True,
        identifier="orders_bq"
    )

    transformed = (
        rows
        | "Trim Customer Name" >> beam.ParDo(TrimValues(["customer_name"]))
        | "Drop Duplicates" >> beam.ParDo(DropDuplicates(["order_id"]))
    )

    transformed | "Write To BQ" >> beam.io.WriteToBigQuery(
        table="my_project:my_dataset.orders_clean",
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    )
```

## Docker Image for Dataflow Custom Container

The repository includes a Dockerfile aligned with Beam SDK `2.72.0` and Python `3.12`.

```bash
docker build -t dojo_beam:3.1.1 .
docker tag dojo_beam:3.1.1 REGION-docker.pkg.dev/PROJECT_ID/dojo-beam/dojo_beam:3.1.1
docker push REGION-docker.pkg.dev/PROJECT_ID/dojo-beam/dojo_beam:3.1.1
```

For Dataflow custom container runs, set:

- `sdk_container_image=REGION-docker.pkg.dev/PROJECT_ID/dojo-beam/dojo_beam:3.1.1`
- `sdk_location=container`

## Release Preparation Checklist (GitHub + PyPI)

1. Confirm versions are aligned in:
   - `setup.py`
   - `pyproject.toml`
   - `enums/DojoBeamTransformVersion.py`
   - Docker image/tag references
2. Build distributions:

```bash
python -m pip install --upgrade build twine
python -m build
python -m twine check dist/*
```

3. Tag and publish GitHub release:

```bash
git tag -a release-v3.1.1 -m "Release v3.1.1"
git push origin release-v3.1.1
```

4. Publish to PyPI:

```bash
python -m twine upload dist/*
```

## Release Text (Ready to Use)

See [RELEASE_TEXT_v3.1.1.md](./RELEASE_TEXT_v3.1.1.md) for prewritten notes for:

- GitHub Release description
- PyPI long-description changelog section
