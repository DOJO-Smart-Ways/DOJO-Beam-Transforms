import apache_beam as beam
import os
from datetime import datetime
import re
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.io import ReadFromBigQuery
import pyarrow as pa
from google.cloud import bigquery
from utils import gcp_utils as gcp


class TruncateBigQueryTableFn(beam.DoFn):
    def __init__(self, project, dataset, table):
        self.gcp_project = project
        self.dataset = dataset
        self.table = table

    def process(self, element):
        client = bigquery.Client(project=self.gcp_project)

        query = (
            f"TRUNCATE TABLE "
            f"`{self.gcp_project}.{self.dataset}.{self.table}`"
        )

        client.query(query).result()

        yield f"Table {self.dataset}.{self.table} truncated."


def get_table_schema(project, dataset, table):
    client = bigquery.Client(project=project)

    table_ref = client.dataset(dataset).table(table)
    table_obj = client.get_table(table_ref)

    return table_obj.schema


def bq_to_pyarrow_schema(bq_schema):
    fields = []

    for field in bq_schema:
        fields.append(
            pa.field(
                field.name,
                bq_field_to_pyarrow_type(field.field_type),
            )
        )

    return pa.schema(fields)


def bq_field_to_pyarrow_type(bq_type):
    if bq_type == "STRING":
        return pa.string()

    elif bq_type in ["FLOAT64", "FLOAT"]:
        return pa.float64()

    elif bq_type in ["INT64", "INTEGER"]:
        return pa.int64()

    elif bq_type == "NUMERIC":
        return pa.decimal128(38, 9)

    elif bq_type in ["BOOL", "BOOLEAN"]:
        return pa.bool_()

    elif bq_type == "TIMESTAMP":
        return pa.timestamp("us")

    elif bq_type == "DATE":
        return pa.date32()

    elif bq_type == "DATETIME":
        return pa.timestamp("us")

    elif bq_type == "TIME":
        return pa.time64("us")

    elif bq_type == "BYTES":
        return pa.binary()

    else:
        raise ValueError(f"Not Allowed type: {bq_type}")


def _normalize_datetime_fields(row, datetime_fields):
    """Convert BigQuery DATETIME values to naive Python datetimes."""
    normalized = row.copy()
    for name in datetime_fields:
        value = row.get(name)
        if value is None:
            continue
        error = (
            f"Field '{name}': expected BigQuery DATETIME "
            "(datetime.datetime without timezone or valid DATETIME string)"
        )
        if isinstance(value, str):
            if not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?",
                value,
            ):
                raise ValueError(error)
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                raise ValueError(error) from None
        if not isinstance(value, datetime) or value.tzinfo is not None:
            raise ValueError(error)
        normalized[name] = value
    return normalized


class BigqueryToRaw:
    def __init__(self):
        self.gcp_project = os.getenv("GCP_PROJECT")
        self.current_date = os.getenv("CURRENT_DATE")

    def run(
        self,
        pipeline: beam.Pipeline,
        identifier: str,
        dataset: str,
        table: str,
        origin_system: str,
        date_column: str = None,
        execution_date: str = None,
        truncate: bool = False,
    ):
        print(f"*********** run {identifier}")

        if not isinstance(pipeline, beam.Pipeline):
            raise TypeError(
                f"Expected apache_beam.Pipeline, got {type(pipeline)}"
            )

        execution_date = execution_date if execution_date else self.current_date

        select_query = (
            f"SELECT * FROM `{self.gcp_project}.{dataset}.{table}`"
        )

        if date_column is not None:
            select_query += (
                f" WHERE CAST({date_column} AS DATE) "
                f"= DATE('{execution_date}')"
            )

        bq_schema = get_table_schema(
            self.gcp_project,
            dataset,
            table,
        )
        pyarrow_schema = bq_to_pyarrow_schema(bq_schema)
        datetime_fields = tuple(
            field.name for field in bq_schema if field.field_type == "DATETIME"
        )

        output_path = gcp.build_gcs_path(
            f"{self.gcp_project}-raw",
            origin_system,
            table,
            execution_date,
            "output",
        )

        result = (
            pipeline
            | f"Read from BigQuery {identifier}"
            >> ReadFromBigQuery(
                query=select_query,
                use_standard_sql=True,
            )
            | f"Normalize DATETIME {identifier}"
            >> beam.Map(_normalize_datetime_fields, datetime_fields)
            | f"Write to Parquet {identifier}"
            >> WriteToParquet(
                file_path_prefix=output_path,
                schema=pyarrow_schema,
                file_name_suffix=".parquet",
            )
        )

        if truncate:
            result = result | (
                f"Truncate BigQuery Table {identifier}"
                >> beam.ParDo(
                    TruncateBigQueryTableFn(
                        self.gcp_project,
                        dataset,
                        table,
                    )
                )
            )

        return result
