import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_cleaning import TrimValues

@pytest.mark.TrimValues
def test_trim_values():
    # Define input data
    input_data = [
        {'name': ' Alice ', 'city': ' New York '},
        {'name': ' Bob ', 'city': ' Los Angeles '}
    ]
    
    # Define expected output
    expected_output = [
        {'name': 'Alice', 'city': 'New York'},
        {'name': 'Bob', 'city': 'Los Angeles'}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply TrimValues' >> beam.ParDo(TrimValues(columns=['name', 'city']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.TrimValues
def test_trim_values_column_not_found():
    # Define input data
    input_data = [
        {'name': ' Alice ', 'city': ' New York '},
        {'name': ' Bob ', 'city': ' Los Angeles '}
    ]
    
    # Define expected output for error handling
    expected_output = [
        {"error": "Column 'non_existent_column' not found in element: {'name': ' Alice ', 'city': ' New York '}"},
        {"error": "Column 'non_existent_column' not found in element: {'name': ' Bob ', 'city': ' Los Angeles '}"}
    ]

    # Run the pipeline and validate the error output
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply TrimValues' >> beam.ParDo(TrimValues(columns=['non_existent_column']))

        # Assert the output contains the error messages
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.TrimValues
def test_trim_values_non_string_value():
    # Define input data
    input_data = [
        {'name': ' Alice ', 'city': 123},
        {'name': ' Bob ', 'city': None}
    ]
    
    # Define expected output for error handling
    expected_output = [
        {"error": "Column 'city' value is not a string: {'name': ' Alice ', 'city': 123}"},
        {"error": "Column 'city' value is not a string: {'name': ' Bob ', 'city': None}"}
    ]

    # Run the pipeline and validate the error output
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply TrimValues' >> beam.ParDo(TrimValues(columns=['city']))

        # Assert the output contains the error messages
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.TrimValues
def test_trim_values_invalid_columns_type():
    # Expect the test to raise a TypeError due to invalid columns type
    with pytest.raises(TypeError, match="Columns must be a list, but got str."):
        TrimValues(columns="invalid_column")

@pytest.mark.TrimValues
def test_trim_values_invalid_columns_content():
    # Expect the test to raise a ValueError due to non-string column in the list
    with pytest.raises(ValueError, match="All columns must be strings."):
        TrimValues(columns=["valid_column", 123])
