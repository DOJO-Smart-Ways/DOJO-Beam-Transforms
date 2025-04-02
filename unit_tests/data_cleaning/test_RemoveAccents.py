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

@pytest.mark.RemoveAccents
def test_remove_accents_invalid_columns_type():
    # Expect the test to raise a TypeError due to invalid columns type
    with pytest.raises(TypeError, match="Columns must be a list, but got str."):
        RemoveAccents(columns="invalid_column")

@pytest.mark.RemoveAccents
def test_remove_accents_invalid_columns_content():
    # Expect the test to raise a ValueError due to non-string column in the list
    with pytest.raises(ValueError, match="All columns must be strings."):
        RemoveAccents(columns=["valid_column", 123])

@pytest.mark.RemoveAccents
def test_remove_accents_column_not_found():
    # Define input data
    input_data = [
        {'name': 'Alice', 'city': 'São Paulo'}
    ]
    
    # Define expected output for error handling
    expected_output = [
        {"error": "Column 'non_existent_column' not found in element: {'name': 'Alice', 'city': 'São Paulo'}"}
    ]

    # Run the pipeline and validate the error output
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply RemoveAccents' >> beam.ParDo(RemoveAccents(columns=['non_existent_column']))

        # Assert the output contains the error message
        assert_that(output_pcoll, equal_to(expected_output))
