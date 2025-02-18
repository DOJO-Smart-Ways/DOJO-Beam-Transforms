import apache_beam as beam
import os
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.io import ReadFromBigQuery
import pyarrow as pa
from google.cloud import bigquery
from utils import gcp_utils as gcp


class TruncateBigQueryTableFn(beam.DoFn):
    def __init__(self, project, dataset, table):
        self.project = project
        self.dataset = dataset
        self.table = table

    def process(self, element):
        client = bigquery.Client(project=self.project)
        query = f"TRUNCATE TABLE `{self.project}.{self.dataset}.{self.table}`"
        client.query(query).result()
        yield f"Tabela {self.dataset}.{self.table} truncada com sucesso."


def get_table_schema(project, dataset, table):
    client = bigquery.Client(project=project)
    table_ref = client.dataset(dataset).table(table)
    table = client.get_table(table_ref)
    return table.schema

def bq_to_pyarrow_schema(bq_schema):
    fields = []
    for field in bq_schema:
        fields.append(pa.field(field.name, bq_field_to_pyarrow_type(field.field_type)))
    return pa.schema(fields)

def bq_field_to_pyarrow_type(bq_type):
    if bq_type == 'STRING':
        return pa.string()
    elif bq_type in ['FLOAT64', 'FLOAT']:
        return pa.float64()
    elif bq_type in ['INT64', 'INTEGER']:
        return pa.float64()
    elif bq_type == 'NUMERIC':
        return pa.decimal128(38, 9)
    elif bq_type == 'BOOL' or bq_type == 'BOOLEAN':
        return pa.bool_()
    elif bq_type == 'TIMESTAMP':
        return pa.timestamp('s')
    elif bq_type == 'DATE':
        return pa.date32()
    elif bq_type == 'DATETIME':
        return pa.timestamp('us')
    elif bq_type == 'TIME':
        return pa.time64('us')
    elif bq_type == 'BYTES':
        return pa.binary()
    else:
        raise ValueError(f"Tipo não suportado: {bq_type}")
    

class BigqueryToRaw:
    def __init__(self):
        self.project = os.getenv('PROJECT')
        self.current_date = os.getenv('CURRENT_DATE')

    def run(self, pipeline, identifier, dataset, table, origin_system, truncate=False):

        print('*********** run ' + identifier)
        select_query = f'SELECT * FROM `{self.project}.{dataset}.{table}`'
        pyarrow_schema = bq_to_pyarrow_schema(get_table_schema(self.project, dataset, table))
        output_path = gcp.build_gcs_path(f'{self.project}-raw', origin_system, table, self.current_date, "output")

        result = (
            pipeline
            | f'Read from BigQuery {identifier}' >> ReadFromBigQuery(query=select_query, use_standard_sql=True)
            #| f'Print elements {identifier}' >> beam.Map(print)
            | f'Write to Parquet {identifier}' >> WriteToParquet(file_path_prefix=output_path,schema=pyarrow_schema, file_name_suffix='.parquet')
            #| f'Truncate BigQuery Table {identifier}' >> beam.ParDo(TruncateBigQueryTableFn(project, dataset, table))
        )

        if truncate:
            result = result | f'Truncate BigQuery Table {identifier}' >> beam.ParDo(TruncateBigQueryTableFn(self.project, dataset, table))

        return result

