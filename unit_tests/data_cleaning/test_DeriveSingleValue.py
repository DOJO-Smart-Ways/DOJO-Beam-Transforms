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

@pytest.mark.DeriveSingleValue
def test_derive_single_value_column_exists():
    # Define input data where the new column already exists
    input_data = [
        {'id': 1, 'name': 'Alice', 'status': 'existing'}
    ]
    
    # Define expected output for error handling
    expected_output = [
        {'error': "Column 'status' already exists in element: {'id': 1, 'name': 'Alice', 'status': 'existing'}"}
    ]

    # Run the pipeline and validate the error output
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply DeriveSingleValue' >> beam.ParDo(DeriveSingleValue('active', 'status'))

        # Assert the output contains the error message
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.DeriveSingleValue
def test_derive_single_value_invalid_type():
    # Expect the test to raise a TypeError due to mismatched value type
    with pytest.raises(TypeError, match="Value 'active' does not match the expected type int"):
        DeriveSingleValue(value='active', new_column='status', value_type=int)

@pytest.mark.DeriveSingleValue
def test_derive_single_value_valid_type():
    # Define input data
    input_data = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'}
    ]
    
    # Define expected output
    expected_output = [
        {'id': 1, 'name': 'Alice', 'status': 1},
        {'id': 2, 'name': 'Bob', 'status': 1}
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply DeriveSingleValue' >> beam.ParDo(DeriveSingleValue(value=1, new_column='status', value_type=int))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))
