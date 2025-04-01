import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from pipeline_components.data_cleaning.RenameColumns import RenameColumns
from unit_tests.utils.csv_reader import read_csv

@pytest.mark.RenameColumns
def test_rename_columns():
    # Read input data from california_housing.csv
    input_data = read_csv('california_housing.csv')
    
    # Define column renaming mapping (example: rename 'longitude' to 'lng' and 'latitude' to 'lat')
    rename_mapping = {'longitude': 'lng', 'latitude': 'lat'}
    
    # Define expected output
    expected_output = [
        {rename_mapping.get(key, key): value for key, value in row.items()}
        for row in input_data
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply RenameColumns' >> beam.ParDo(RenameColumns(rename_mapping))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))
