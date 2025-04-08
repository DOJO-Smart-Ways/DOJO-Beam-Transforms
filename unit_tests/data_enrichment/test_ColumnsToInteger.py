import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from pipeline_components.data_enrichment.ColumnsToInteger import ColumnsToInteger

@pytest.mark.ColumnsToInteger
def test_columns_to_integer():
    # Input data
    input_data = [
        {'age': '25', 'score': '100.5'},
        {'age': 'invalid', 'score': '50'},
        {'age': '', 'score': None},
        {'age': '30', 'score': '75'}
    ]

    # Define columns to convert
    columns_to_convert = ['age', 'score']

    # Define expected output
    expected_output = [
        {'age': 25, 'score': 100},
        {'age': 'invalid', 'score': '50'},
        {'age': '', 'score': None},
        {'age': 30, 'score': 75}
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert to Integer' >> beam.ParDo(ColumnsToInteger(columns=columns_to_convert))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ColumnsToInteger
def test_columns_to_integer_error():
    # Input data with invalid values
    input_data = [
        {'age': 'invalid', 'score': '50'},
        {'age': '', 'score': None}
    ]

    # Run the pipeline and expect an error
    with pytest.raises(ValueError, match="Error converting column 'age' to Integer"):
        with TestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            output_pcoll = input_pcoll | 'Convert to Integer' >> beam.ParDo(ColumnsToInteger(columns=['age']))

@pytest.mark.ColumnsToInteger
def test_columns_to_integer_invalid_columns_type():
    # Expect the test to raise a TypeError due to invalid columns type
    with pytest.raises(TypeError, match="Columns must be a list, but got str."):
        ColumnsToInteger(columns="invalid_column")

@pytest.mark.ColumnsToInteger
def test_columns_to_integer_invalid_columns_content():
    # Expect the test to raise a ValueError due to non-string column in the list
    with pytest.raises(ValueError, match="All columns must be strings."):
        ColumnsToInteger(columns=["valid_column", 123])

@pytest.mark.ColumnsToInteger
def test_columns_to_integer_missing_column():
    # Define input data
    input_data = [
        {'id': 1, 'name': 'Alice', 'age': 30},
        {'id': 2, 'name': 'Bob', 'age': 25}
    ]

    # Define columns to convert
    columns_to_convert = ['non_existent_column']

    # Define expected output
    expected_output = [
        {},  # No matching columns
        {}
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert to Integer' >> beam.ParDo(ColumnsToInteger(columns=columns_to_convert))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ColumnsToInteger
def test_columns_to_integer_missing_column_error():
    # Input data
    input_data = [
        {'id': 1, 'name': 'Alice', 'age': 30},
        {'id': 2, 'name': 'Bob', 'age': 25}
    ]

    # Run the pipeline and expect a KeyError
    with pytest.raises(KeyError, match="Column 'non_existent_column' not found in the input element"):
        with TestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            output_pcoll = input_pcoll | 'Convert to Integer' >> beam.ParDo(ColumnsToInteger(columns=['non_existent_column']))