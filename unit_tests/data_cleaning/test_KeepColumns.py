import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
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
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply KeepColumns' >> beam.ParDo(KeepColumns(columns_to_keep))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))
