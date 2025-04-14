import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment.SplitColumn import SplitColumn

def test_split_column():
    # Define input data
    input_data = [
        {'field1': 'value1,value2,value3'},
        {'field1': 'value4,value5'},
    ]

    # Define expected output
    expected_output = [
        {'field1': 'value1,value2,value3', 'field2': ['value1', 'value2', 'value3']},
        {'field1': 'value4,value5', 'field2': ['value4', 'value5']},
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply SplitColumn' >> beam.ParDo(
            SplitColumn(
                column_to_split='field1',
                new_columns=['field2'],
                delimiter=','
            )
        )
        assert_that(output_pcoll, equal_to(expected_output))


def test_split_column_as_list():
    # Define input data
    input_data = [
        {'field1': 'value1,value2,value3'},
        {'field1': 'value4,value5'},
    ]

    # Define expected output
    expected_output = [
        {'field1': 'value1,value2,value3', 'field2': ['value1', 'value2', 'value3']},
        {'field1': 'value4,value5', 'field2': ['value4', 'value5']},
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply SplitColumn' >> beam.ParDo(
            SplitColumn(
                column_to_split='field1',
                new_columns=['field2'],
                delimiter=',',
                as_list=True
            )
        )
        assert_that(output_pcoll, equal_to(expected_output))


def test_split_column_to_multiple_columns():
    # Define input data
    input_data = [
        {'field1': 'value1,value2,value3'},
        {'field1': 'value4,value5'},
    ]

    # Define expected output
    expected_output = [
        {'field1': 'value1,value2,value3', 'field2': 'value1', 'field3': 'value2', 'field4': 'value3'},
        {'field1': 'value4,value5', 'field2': 'value4', 'field3': 'value5', 'field4': None},
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply SplitColumn' >> beam.ParDo(
            SplitColumn(
                column_to_split='field1',
                new_columns=['field2', 'field3', 'field4'],
                delimiter=',',
                as_list=False
            )
        )
        assert_that(output_pcoll, equal_to(expected_output))