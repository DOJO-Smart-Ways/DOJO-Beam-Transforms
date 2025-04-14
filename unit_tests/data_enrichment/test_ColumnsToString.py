import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline_components.data_enrichment.ColumnsToString import ColumnsToString

@pytest.mark.ColumnsToString
def test_columns_to_string_conversion():
    # Input data
    input_data = [
        {'name': 'Alice', 'age': 30, 'score': None},
        {'name': 'Bob', 'age': 25, 'score': 85.5},
        {'name': None, 'age': '40', 'score': ''}
    ]

    # Expected output
    expected_output = [
        {'name': 'Alice', 'age': '30', 'score': None},
        {'name': 'Bob', 'age': '25', 'score': '85.5'},
        {'name': None, 'age': '40', 'score': ''}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert to String' >> beam.ParDo(ColumnsToString(columns=['name', 'age', 'score']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ColumnsToString
def test_columns_to_string_missing_column():
    # Input data
    input_data = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'}
    ]
    #f Run the pipeline

    with pytest.raises(KeyError, match=r"Column 'non_existent_column' not found in the input element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Convert to String' >> beam.ParDo(ColumnsToString(columns=['non_existent_column']))