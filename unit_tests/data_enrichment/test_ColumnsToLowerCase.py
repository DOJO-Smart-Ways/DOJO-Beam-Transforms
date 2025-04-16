import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline_components.data_enrichment import ColumnsToLowerCase

@pytest.mark.ColumnsToLowerCase
def test_columns_to_lowercase_conversion():
    """
    Test if ColumnsToLowerCase correctly converts specified columns to lowercase.
    """
    # Input data
    input_data = [
        {'name': 'ALICE', 'city': 'NEW YORK'},
        {'name': 'BOB', 'city': 'LOS ANGELES'},
        {'name': 'CHARLIE', 'city': 'SAN FRANCISCO'}
    ]

    # Expected output
    expected_output = [
        {'name': 'alice', 'city': 'new york'},
        {'name': 'bob', 'city': 'los angeles'},
        {'name': 'charlie', 'city': 'san francisco'}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert to Lowercase' >> beam.ParDo(ColumnsToLowerCase(columns=['name', 'city']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))


@pytest.mark.ColumnsToLowerCase
def test_columns_to_lowercase_missing_column():
    """
    Test if ColumnsToLowerCase raises an error when a specified column is missing.
    """
    # Input data
    input_data = [
        {'name': 'ALICE', 'city': 'NEW YORK'},
        {'name': 'BOB', 'city': 'LOS ANGELES'}
    ]

    # Run the pipeline and expect a KeyError
    with pytest.raises(KeyError, match=r"Column 'non_existent_column' not found in the input element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Convert to Lowercase' >> beam.ParDo(ColumnsToLowerCase(columns=['non_existent_column']))


@pytest.mark.ColumnsToLowerCase
def test_columns_to_lowercase_invalid_columns_type():
    """
    Test if ColumnsToLowerCase raises a TypeError when columns argument is not a list.
    """
    with pytest.raises(TypeError, match="Columns must be a list, but got str."):
        ColumnsToLowerCase(columns="invalid_column")


@pytest.mark.ColumnsToLowerCase
def test_columns_to_lowercase_invalid_columns_content():
    """
    Test if ColumnsToLowerCase raises a ValueError when columns list contains non-string values.
    """
    with pytest.raises(ValueError, match="All columns must be strings."):
        ColumnsToLowerCase(columns=["valid_column", 123])