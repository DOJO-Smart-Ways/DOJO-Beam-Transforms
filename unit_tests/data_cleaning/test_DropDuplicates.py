import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_cleaning import DropDuplicates

@pytest.mark.DropDuplicates
def test_drop_duplicates():
    # Define input data
    input_data = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'},
        {'id': 1, 'name': 'Alice'}  # Duplicate
    ]
    
    # Define expected output
    expected_output = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply DropDuplicates' >> beam.ParDo(DropDuplicates(columns=['id', 'name']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.DropDuplicates
def test_drop_duplicates_non_dict_element():
    # Define input data with a non-dictionary element
    input_data = [
        {'id': 1, 'name': 'Alice'},
        ['invalid', 'element']
    ]
    
    # Define expected output for error handling
    expected_output = [
        {'error': "Element is not a dictionary: ['invalid', 'element']"}
    ]

    # Run the pipeline and validate the error output
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply DropDuplicates' >> beam.ParDo(DropDuplicates(columns=['id', 'name']))

        # Assert the output contains the error message
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.DropDuplicates
def test_drop_duplicates_missing_column():
    # Define input data where a column is missing
    input_data = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2}  # Missing 'name'
    ]
    
    # Define expected output for error handling
    expected_output = [
        {'error': "Column 'name' not found in element: {'id': 2}"}
    ]

    # Run the pipeline and validate the error output
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply DropDuplicates' >> beam.ParDo(DropDuplicates(columns=['id', 'name']))

        # Assert the output contains the error message
        assert_that(output_pcoll, equal_to(expected_output))
