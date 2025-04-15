import pytest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment import MergeColumns

@pytest.mark.MergeColumns
def test_merge_columns():
    # Define input data
    input_data = [
        {'field1': 'value1', 'field2': 'value2'},
        {'field1': 'value3', 'field2': 'value4'},
    ]

    # Define expected output
    expected_output = [
        {'field1': 'value1', 'field2': 'value2', 'merged_field': 'value1-value2'},
        {'field1': 'value3', 'field2': 'value4', 'merged_field': 'value3-value4'},
    ]

    params = [
        (['field1', 'field2'], 'merged_field', '-')
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply MergeColumns' >> beam.ParDo(
            MergeColumns(
                params
            )
        )
        assert_that(output_pcoll, equal_to(expected_output))


@pytest.mark.MergeColumns
def test_merge_columns_target_column_already_exists():
    # Input data
    input_data = [
        {'field1': 'value1', 'field2': 'value2', 'merged_field': 'existing_value'}
    ]

    # Define parameters
    params = [
        (['field1', 'field2'], 'merged_field', '-')
    ]

    # Run the pipeline and expect a ValueError
    with pytest.raises(ValueError, match="Target column 'merged_field' already exists in the element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Apply MergeColumns' >> beam.ParDo(MergeColumns(params))


@pytest.mark.MergeColumns
def test_merge_columns_missing_source_column():
    # Input data
    input_data = [
        {'field1': 'value1'}  # Missing 'field2'
    ]

    # Define parameters
    params = [
        (['field1', 'field2'], 'merged_field', '-')
    ]

    # Run the pipeline and expect a KeyError
    with pytest.raises(KeyError, match="Missing column 'field2' in the element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Apply MergeColumns' >> beam.ParDo(MergeColumns(params))