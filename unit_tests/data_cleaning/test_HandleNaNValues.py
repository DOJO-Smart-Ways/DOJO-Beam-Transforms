import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline_components.data_cleaning import HandleNaNValues
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
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply HandleNaNValues' >> beam.ParDo(HandleNaNValues(default_value='default'))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))


@pytest.mark.HandleNaNValues
def test_handle_nan_values_invalid_strategy():
    # Expect the test to raise a ValueError due to invalid strategy
    with pytest.raises(ValueError, match="Invalid strategy 'invalid_strategy'. Supported strategies are 'replace' or 'remove'."):
        HandleNaNValues(strategy='invalid_strategy', default_value='default')


@pytest.mark.HandleNaNValues
def test_handle_nan_values_invalid_columns_type():
    # Expect the test to raise a TypeError due to invalid columns type
    with pytest.raises(TypeError, match="Columns must be a list, but got str."):
        HandleNaNValues(strategy='replace', default_value='default', columns='invalid_column')


@pytest.mark.HandleNaNValues
def test_handle_nan_values_invalid_columns_content():
    # Expect the test to raise a ValueError due to non-string column in the list
    with pytest.raises(ValueError, match="All columns must be strings."):
        HandleNaNValues(strategy='replace', default_value='default', columns=['valid_column', 123])