import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline_components.data_enrichment import ColumnsToUpperCase

@pytest.mark.ColumnsToUpperCase
def test_columns_to_uppercase_conversion():
    """
    Test if ColumnsToUpperCase correctly converts specified columns to uppercase.
    """
    # Input data
    input_data = [
        {'name': 'alice', 'city': 'new york'},
        {'name': 'bob', 'city': 'los angeles'},
        {'name': 'charlie', 'city': 'san francisco'}
    ]

    # Expected output
    expected_output = [
        {'name': 'ALICE', 'city': 'NEW YORK'},
        {'name': 'BOB', 'city': 'LOS ANGELES'},
        {'name': 'CHARLIE', 'city': 'SAN FRANCISCO'}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert to Uppercase' >> beam.ParDo(ColumnsToUpperCase(columns=['name', 'city']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))


@pytest.mark.ColumnsToUpperCase
def test_columns_to_uppercase_missing_column():
    """
    Test if ColumnsToUpperCase raises an error when a specified column is missing.
    """
    # Input data
    input_data = [
        {'name': 'alice', 'city': 'new york'},
        {'name': 'bob', 'city': 'los angeles'}
    ]

    # Run the pipeline and expect a KeyError
    with pytest.raises(KeyError, match=r"Column 'non_existent_column' not found in the input element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Convert to Uppercase' >> beam.ParDo(ColumnsToUpperCase(columns=['non_existent_column']))


@pytest.mark.ColumnsToUpperCase
def test_columns_to_uppercase_invalid_columns_type():
    """
    Test if ColumnsToUpperCase raises a TypeError when columns argument is not a list.
    """
    with pytest.raises(TypeError, match="Columns must be a list, but got str."):
        ColumnsToUpperCase(columns="invalid_column")


@pytest.mark.ColumnsToUpperCase
def test_columns_to_uppercase_invalid_columns_content():
    """
    Test if ColumnsToUpperCase raises a ValueError when columns list contains non-string values.
    """
    with pytest.raises(ValueError, match="All columns must be strings."):
        ColumnsToUpperCase(columns=["valid_column", 123])