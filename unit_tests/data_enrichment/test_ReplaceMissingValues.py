import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment.ReplaceMissingValues import ReplaceMissingValues
import pytest

def test_replace_missing_values():
    # Define input data
    input_data = [
        {'field1': 'value1', 'field2': None},
        {'field1': None, 'field2': 123},
    ]

    # Define expected output
    expected_output = [
        {'field1': 'value1', 'field2': 'default_value'},
        {'field1': 'default_value', 'field2': 123},
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply ReplaceMissingValues' >> beam.ParDo(
            ReplaceMissingValues(
                replacements={'field1': 'default_value', 'field2': 'default_value'}
            )
        )
        assert_that(output_pcoll, equal_to(expected_output))


def test_replace_missing_column():
    # Define input data
    input_data = [
        {'field1': 'value1'},  # 'field2' is absent
        {'field1': None},      # 'field2' is absent
    ]

    # Define expected output
    expected_output = [
        {'field1': 'value1', 'field2': 'default_value'},
        {'field1': 'default_value', 'field2': 'default_value'},
    ]

    # Run the pipeline
    with pytest.raises(KeyError, match=r"Column 'field2' not found in the input element: \{'field1': 'value1'\}.*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            output_pcoll = input_pcoll | 'Apply ReplaceMissingValues' >> beam.ParDo(
                ReplaceMissingValues(
                    replacements={'field1': 'default_value', 'field2': 'default_value'}
                )
            )
            assert_that(output_pcoll, equal_to(expected_output))


def test_replace_missing_values_no_change():
    # Define input data
    input_data = [
        {'field1': 'value1', 'field2': 123},
        {'field1': 'value2', 'field2': 'value3'},
    ]

    # Define expected output (no changes expected)
    expected_output = [
        {'field1': 'value1', 'field2': 123},
        {'field1': 'value2', 'field2': 'value3'},
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply ReplaceMissingValues' >> beam.ParDo(
            ReplaceMissingValues(
                replacements={'field1': 'default_value', 'field2': 'default_value'}
            )
        )
        assert_that(output_pcoll, equal_to(expected_output))