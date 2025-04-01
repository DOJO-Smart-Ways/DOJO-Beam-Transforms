import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from pipeline_components.data_cleaning.HandleNaNValues import HandleNaNValues
from unit_tests.utils.csv_reader import read_csv


@pytest.mark.HandleNaNValues
def test_handle_nan_values():
    # Read input data from california_housing.csv
    input_data = read_csv('california_housing.csv')
    
    # Define expected output (example: replacing NaN values with a default value)
    expected_output = [
        {key: (value if value != '' else 'default') for key, value in row.items()}
        for row in input_data
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply HandleNaNValues' >> beam.ParDo(HandleNaNValues(default_value='default'))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))