import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from pipeline_components.data_cleaning.ReplaceValues import ReplaceValues

@pytest.mark.ReplaceValues
def test_replace_values():
    # Define input data
    input_data = [
        {'status': 'inactive', 'city': 'New York'},
        {'status': 'active', 'city': 'Los Angeles'},
        {'status': 'inactive', 'city': 'Chicago'}
    ]
    
    # Define replacements
    replacements = {'inactive': 'disabled', 'active': 'enabled'}
    
    # Define expected output
    expected_output = [
        {'status': 'disabled', 'city': 'New York'},
        {'status': 'enabled', 'city': 'Los Angeles'},
        {'status': 'disabled', 'city': 'Chicago'}
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply ReplaceValues' >> beam.ParDo(ReplaceValues(columns=['status'], replacements=replacements))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ReplaceValues
def test_replace_values_column_not_found():
    # Define input data
    input_data = [
        {'status': 'inactive', 'city': 'New York'},
        {'status': 'active', 'city': 'Los Angeles'}
    ]
    
    # Define replacements
    replacements = {'inactive': 'disabled', 'active': 'enabled'}
    
    # Define expected output for error handling
    expected_output = [
        {"error": "Column 'non_existent_column' not found in element: {'status': 'inactive', 'city': 'New York'}"},
        {"error": "Column 'non_existent_column' not found in element: {'status': 'active', 'city': 'Los Angeles'}"}
    ]

    # Run the pipeline and validate the error output
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply ReplaceValues' >> beam.ParDo(ReplaceValues(columns=['non_existent_column'], replacements=replacements))

        # Assert the output contains the error messages
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ReplaceValues
def test_replace_values_invalid_columns_type():
    # Expect the test to raise a TypeError due to invalid columns type
    with pytest.raises(TypeError, match="Columns must be a list, but got str."):
        ReplaceValues(columns="invalid_column", replacements={'key': 'value'})

@pytest.mark.ReplaceValues
def test_replace_values_invalid_columns_content():
    # Expect the test to raise a ValueError due to non-string column in the list
    with pytest.raises(ValueError, match="All columns must be strings."):
        ReplaceValues(columns=["valid_column", 123], replacements={'key': 'value'})

@pytest.mark.ReplaceValues
def test_replace_values_invalid_replacements_type():
    # Expect the test to raise a TypeError due to invalid replacements type
    with pytest.raises(TypeError, match="Replacements must be a dictionary, but got list."):
        ReplaceValues(columns=['status'], replacements=['invalid_replacement'])
