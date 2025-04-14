# tests/test_gcp_utils.py

import pytest
from utils.gcp_utils import build_gcs_path, build_bq_schema

# Section: Tests for build_gcs_path function
@pytest.mark.build_gcs_path
def test_build_gcs_path_with_arguments():
    """
    Test build_gcs_path with valid arguments.

    Ensures that the function correctly constructs a GCS path
    when provided with valid arguments.
    """
    result = build_gcs_path("bucket", "path", "to", "file")
    assert result == "gs://bucket/path/to/file"


@pytest.mark.build_gcs_path
def test_build_gcs_path_no_arguments():
    """
    Test build_gcs_path with no arguments.

    Ensures that the function raises a ValueError when no arguments are provided.
    """
    with pytest.raises(ValueError, match="At least one argument must be provided."):
        build_gcs_path()


# Section: Tests for build_bq_schema function

@pytest.mark.build_bq_schema
def test_build_bq_schema_valid():
    """
    Test build_bq_schema with valid input.

    Ensures that the function correctly constructs a BigQuery schema
    when provided with valid column tuples.
    """
    result = build_bq_schema([("name", "STRING"), ("age", "INT64")])
    expected = {'fields': [{'name': 'name', 'type': 'STRING'}, {'name': 'age', 'type': 'INT64'}]}
    assert result == expected


@pytest.mark.build_bq_schema
def test_build_bq_schema_empty():
    """
    Test build_bq_schema with an empty list.

    Ensures that the function raises a ValueError when provided with an empty list.
    """
    with pytest.raises(ValueError, match="Column tuples list must not be empty."):
        build_bq_schema([])


@pytest.mark.build_bq_schema
def test_build_bq_schema_invalid_tuple_first_args():
    """
    Test build_bq_schema with an invalid tuple.

    Ensures that the function raises a ValueError when provided with a tuple
    that does not contain a string column name and a string data type.
    """
    with pytest.raises(ValueError, match="Each tuple must contain a column name and data type as strings."):
        build_bq_schema([("name", "STRING"), (123, "INT64")])


@pytest.mark.build_bq_schema
def test_build_bq_schema_invalid_tuple_second_args():
    """
    Test build_bq_schema with an invalid type for data type.

    Ensures that the function raises a ValueError when the data type is not a string.
    """
    with pytest.raises(ValueError, match="Each tuple must contain a column name and data type as strings."):
        build_bq_schema([("name", "STRING"), ("age", 123)])


@pytest.mark.build_bq_schema
def test_build_bq_schema_invalid_data_type():
    """
    Test build_bq_schema with an unsupported data type.

    Ensures that the function raises a ValueError when provided with an unsupported data type.
    """
    valid_types = {
        "ARRAY", "BOOL", "BYTES", "DATE", "DATETIME", "GEOGRAPHY", "INTERVAL", "JSON",
        "INT64", "INT", "SMALLINT", "INTEGER", "BIGINT", "TINYINT", "BYTEINT", "NUMERIC",
        "DECIMAL", "BIGNUMERIC", "BIGDECIMAL", "FLOAT64", "FLOAT", "RANGE", "STRING", "STRUCT",
        "TIME", "TIMESTAMP"
    }

    column_name = "test_column"
    invalid_type = "INVALID_TYPE"
    with pytest.raises(ValueError, match=f"Invalid data type '{invalid_type}' in column '{column_name}'. Supported types are: {', '.join(sorted(valid_types))}"):
        build_bq_schema([("name", "STRING"), (column_name, invalid_type)])
