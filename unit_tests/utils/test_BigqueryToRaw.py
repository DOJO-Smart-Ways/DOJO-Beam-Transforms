from datetime import datetime, timezone
import importlib

import apache_beam as beam
from google.cloud.bigquery import SchemaField
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from utils.BigqueryToRaw import _normalize_datetime_fields, bq_to_pyarrow_schema


@pytest.mark.parametrize('value, expected', [
    ('2026-11-10 12:01:51', datetime(2026, 11, 10, 12, 1, 51)),
    ('2026-11-10T12:01:51', datetime(2026, 11, 10, 12, 1, 51)),
    ('2026-11-10 12:01:51.123456', datetime(2026, 11, 10, 12, 1, 51, 123456)),
    ('2026-11-10T12:01:51.1', datetime(2026, 11, 10, 12, 1, 51, 100000)),
    (None, None),
    (datetime(2026, 11, 10, 12, 1, 51, 123456), datetime(2026, 11, 10, 12, 1, 51, 123456)),
])
def test_normalize_datetime(value, expected):
    row = {'CPUDT': value, 'text': '2026-11-10 12:01:51', 'count': 7, 'other': None}
    result = _normalize_datetime_fields(row, ('CPUDT',))
    assert result == {**row, 'CPUDT': expected}
    assert row['CPUDT'] is value
    if isinstance(value, datetime):
        assert result['CPUDT'] is value


@pytest.mark.parametrize('value', [
    'private-invalid-value', '2026-02-30 12:00:00', 123, [],
    '2026-11-10T12:01:51Z', '2026-11-10T12:01:51+03:00',
    '2026-11-10T12:01:51.1234567', datetime(2026, 11, 10, tzinfo=timezone.utc),
])
def test_invalid_datetime_has_safe_error(value):
    with pytest.raises(ValueError) as exc:
        _normalize_datetime_fields({'CPUDT': value, 'secret': 'private-record'}, ('CPUDT',))
    assert 'CPUDT' in str(exc.value)
    assert 'DATETIME' in str(exc.value)
    assert 'private' not in str(exc.value)
    assert exc.value.__suppress_context__ or exc.value.__context__ is None


def test_missing_field_is_preserved():
    assert _normalize_datetime_fields({'text': 'unchanged'}, ('CPUDT',)) == {'text': 'unchanged'}


def test_bigquery_to_parquet_local(monkeypatch, tmp_path):
    module = importlib.import_module('utils.BigqueryToRaw')
    bq_schema = [SchemaField('CPUDT', 'DATETIME'), SchemaField('label', 'STRING'), SchemaField('count', 'INTEGER')]
    expected_schema = pa.schema([('CPUDT', pa.timestamp('us')), ('label', pa.string()), ('count', pa.int64())])
    schema = bq_to_pyarrow_schema(bq_schema)
    assert schema == expected_schema
    rows = [
        {'CPUDT': '2026-11-10 12:01:51.123456', 'label': 'a', 'count': 1},
        {'CPUDT': '2026-11-10T12:01:51.1', 'label': 'b', 'count': 2},
        {'CPUDT': None, 'label': 'c', 'count': 3},
        {'CPUDT': datetime(2026, 11, 10), 'label': 'd', 'count': 4},
    ]
    expected = [_normalize_datetime_fields(row, ('CPUDT',)) for row in rows]
    arrow_table = pa.Table.from_pylist(expected, schema=schema)
    direct_path = tmp_path / 'direct.parquet'
    pq.write_table(arrow_table, direct_path)
    restored = pq.read_table(direct_path)
    assert restored.schema == expected_schema
    assert restored.to_pylist() == expected

    schema_calls = []
    def get_schema(*args):
        schema_calls.append(args)
        return bq_schema

    read_calls = []
    def read(**kwargs):
        read_calls.append(kwargs)
        return beam.Create(rows)

    monkeypatch.setattr(module, 'get_table_schema', get_schema)
    monkeypatch.setattr(module, 'ReadFromBigQuery', read)
    monkeypatch.setattr(module.gcp, 'build_gcs_path', lambda *args: str(tmp_path / 'beam-output'))
    monkeypatch.setenv('GCP_PROJECT', 'local-project')
    with beam.Pipeline() as pipeline:
        module.BigqueryToRaw().run(pipeline, 'local', 'dataset', 'MSEG', 'origin', execution_date='2026-11-10')
    assert schema_calls == [('local-project', 'dataset', 'MSEG')]
    assert read_calls == [{'query': 'SELECT * FROM `local-project.dataset.MSEG`', 'use_standard_sql': True}]
    files = list(tmp_path.glob('beam-output*.parquet'))
    assert files
    restored = pq.read_table(files)
    assert restored.schema == expected_schema
    assert sorted(restored.to_pylist(), key=lambda row: row['count']) == expected
