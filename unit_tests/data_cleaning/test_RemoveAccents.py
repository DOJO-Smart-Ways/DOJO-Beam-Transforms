import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_cleaning.RemoveAccents import RemoveAccents

@pytest.mark.RemoveAccents
def test_remove_accents():
    # Example: Assume 'city_name' column contains accented characters
    input_data_test = [
        {'city_name': 'São Paulo'},
        {'city_name': 'Tóquio'},
        {'city_name': 'Bàhia'},
        {'city_name': 'München'},
        {'city_name': 'Zürich'},
        {'city_name': 'São Paulo'},
        {'city_name': 'Santiagô'}
    ]
    
    # Define expected output (accents removed from 'city_name')
    expected_output = [
        {'city_name': 'Sao Paulo'},
        {'city_name': 'Toquio'},
        {'city_name': 'Bahia'},
        {'city_name': 'Munchen'},
        {'city_name': 'Zurich'},
        {'city_name': 'Sao Paulo'},
        {'city_name': 'Santiago'}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data_test)
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
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply RemoveAccents' >> beam.ParDo(RemoveAccents(columns=['non_existent_column']))

        # Assert the output contains the error message
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.RemoveAccents
def test_remove_accents_single_column():
    # Define input data with multiple columns
    input_data = [
        {'city_name': 'São Paulo', 'country': 'Brasil'},
        {'city_name': 'Tóquio', 'country': 'Japão'},
        {'city_name': 'Bàhia', 'country': 'Brasil'}
    ]

    # Define expected output (accents removed only from 'city_name')
    expected_output = [
        {'city_name': 'Sao Paulo', 'country': 'Brasil'},
        {'city_name': 'Toquio', 'country': 'Japão'},
        {'city_name': 'Bahia', 'country': 'Brasil'}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply RemoveAccents' >> beam.ParDo(RemoveAccents(columns=['city_name']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))
