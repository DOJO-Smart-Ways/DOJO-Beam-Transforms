import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from pipeline_components.data_cleaning.RemoveAccents import RemoveAccents
from unit_tests.utils.csv_reader import read_csv

@pytest.mark.RemoveAccents
def test_remove_accents():
    # Read input data from california_housing.csv
    input_data = read_csv('california_housing.csv')
    
    # Example: Assume 'city_name' column contains accented characters
    input_data = [
        {**row, 'city_name': 'São Paulo'} if 'city_name' in row else row
        for row in input_data
    ]
    
    # Define expected output (accents removed from 'city_name')
    expected_output = [
        {**row, 'city_name': 'Sao Paulo'} if 'city_name' in row else row
        for row in input_data
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply RemoveAccents' >> beam.ParDo(RemoveAccents(columns=['city_name']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))
