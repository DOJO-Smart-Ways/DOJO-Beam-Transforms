import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment import ColumnCopy

def test_column_copy():
    # Define input data
    input_data = [
        {'field1': 'value1', 'field2': 123},
        {'field1': 'value2', 'field2': 456},
    ]

    # Define expected output
    expected_output = [
        {'field1': 'value1', 'field2': 123, 'field3': 'value1'},
        {'field1': 'value2', 'field2': 456, 'field3': 'value2'},
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply ColumnCopy' >> beam.ParDo(
            ColumnCopy(
            source_column='field1',
            target_column='field3'
        ))
        assert_that(output_pcoll, equal_to(expected_output))