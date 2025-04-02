import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from pipeline_components.data_cleaning.DropColumns import DropColumns
from unit_tests.utils.csv_reader import read_csv

def test_drop_columns():
    # Read input data from california_housing.csv
    input_data = read_csv('california_housing.csv')
    
    # Define expected output
    expected_output = [
        {key: value for key, value in row.items() if key != 'median_income'}
        for row in input_data
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply DropColumns' >> beam.ParDo(DropColumns(['median_income']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

def test_drop_columns_missing_column():
    # Define input data where the column to be dropped does not exist
    input_data = [
        {'id': 1, 'name': 'Alice'}
    ]
    
    # Define expected output for error handling
    expected_output = [
        {'error': "Column 'non_existent_column' not found in element: {'id': 1, 'name': 'Alice'}"}
    ]

    # Run the pipeline and validate the error output
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply DropColumns' >> beam.ParDo(DropColumns(column=['non_existent_column']))

        # Assert the output contains the error message
        assert_that(output_pcoll, equal_to(expected_output))
