import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from pipeline_components.data_cleaning.DeriveSingleValue import DeriveSingleValue
from unit_tests.utils.csv_reader import read_csv

@pytest.mark.DeriveSingleValue
def test_derive_single_value():
    # Read input data from california_housing.csv
    input_data = read_csv('california_housing.csv')
    
    # Define expected output
    expected_output = [
        {**row, 'status': 'active'} for row in input_data
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply DeriveSingleValue' >> beam.ParDo(DeriveSingleValue('active', 'status'))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))
