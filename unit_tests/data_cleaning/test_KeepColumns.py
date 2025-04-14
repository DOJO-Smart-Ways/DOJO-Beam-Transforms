import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_cleaning.KeepColumns import KeepColumns
from unit_tests.utils.csv_reader import read_csv

@pytest.mark.KeepColumns
def test_keep_columns():
    # Read input data from california_housing.csv
    input_data = read_csv('california_housing.csv')
    
    # Define columns to keep (example: 'longitude' and 'latitude')
    columns_to_keep = ['longitude', 'latitude']
    
    # Define expected output
    expected_output = [
        {key: value for key, value in row.items() if key in columns_to_keep}
        for row in input_data
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply KeepColumns' >> beam.ParDo(KeepColumns(columns_to_keep))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.KeepColumns
def test_keep_columns_invalid_columns_type():
    # Expect the test to raise a TypeError due to invalid columns type
    with pytest.raises(TypeError, match="Columns must be a list, but got str."):
        KeepColumns(columns="invalid_column")

@pytest.mark.KeepColumns
def test_keep_columns_invalid_columns_content():
    # Expect the test to raise a ValueError due to non-string column in the list
    with pytest.raises(ValueError, match="All columns must be strings."):
        KeepColumns(columns=["valid_column", 123])

@pytest.mark.KeepColumns
def test_keep_columns_missing_column():
    # Define input data
    input_data = [
        {'id': 1, 'name': 'Alice', 'age': 30},
        {'id': 2, 'name': 'Bob', 'age': 25}
    ]
    
    # Define columns to keep
    columns_to_keep = ['non_existent_column']
    
    # Define expected output
    expected_output = [
        {},  # No matching columns
        {}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply KeepColumns' >> beam.ParDo(KeepColumns(columns=columns_to_keep))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))
