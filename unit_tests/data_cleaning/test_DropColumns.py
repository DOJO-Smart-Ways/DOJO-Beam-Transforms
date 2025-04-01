import apache_beam as beam
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
