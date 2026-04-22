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
    
    # Run the pipeline and validate it fails with the expected message
    with pytest.raises(RuntimeError, match=r"Element is not a dictionary: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Apply DropDuplicates' >> beam.ParDo(DropDuplicates(columns=['id', 'name']))

@pytest.mark.DropDuplicates
def test_drop_duplicates_missing_column():
    # Define input data where a column is missing
    input_data = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2}  # Missing 'name'
    ]
    
    # Run the pipeline and validate it fails with the expected message
    with pytest.raises(RuntimeError, match=r"Column 'name' not found in element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Apply DropDuplicates' >> beam.ParDo(DropDuplicates(columns=['id', 'name']))
