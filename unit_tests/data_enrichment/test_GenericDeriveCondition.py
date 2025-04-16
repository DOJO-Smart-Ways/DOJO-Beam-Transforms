import pytest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment import GenericDeriveCondition

@pytest.mark.GenericDeriveCondition
def test_generic_derive_condition():
    # Define input data
    input_data = [
        {'field1': 'apple', 'field2': 'fruit'},
        {'field1': 'carrot', 'field2': 'vegetable'},
        {'field1': 'unknown', 'field2': 'other'},  # No match in the map
    ]

    # Define expected output
    expected_output = [
        {'field1': 'apple', 'field2': 'fruit', 'derived_field': 'sweet'},
        {'field1': 'carrot', 'field2': 'vegetable', 'derived_field': 'healthy'},
        {'field1': 'unknown', 'field2': 'other', 'derived_field': 'default'},  # Default value
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply GenericDeriveCondition' >> beam.ParDo(
            GenericDeriveCondition(
                column='field1',
                map={'apple': 'sweet', 'carrot': 'healthy'},
                new_column='derived_field'
            )
        )
        assert_that(output_pcoll, equal_to(expected_output))


@pytest.mark.GenericDeriveCondition
def test_generic_derive_condition_missing_source_column():
    # Input data
    input_data = [
        {'field2': 'fruit'}  # Missing 'field1'
    ]

    # Run the pipeline and expect a KeyError
    with pytest.raises(KeyError, match="Source column 'field1' not found in the element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Apply GenericDeriveCondition' >> beam.ParDo(
                GenericDeriveCondition(
                    column='field1',
                    map={'apple': 'sweet', 'carrot': 'healthy'},
                    new_column='derived_field'
                )
            )


@pytest.mark.GenericDeriveCondition
def test_generic_derive_condition_target_column_already_exists():
    # Input data
    input_data = [
        {'field1': 'apple', 'field2': 'fruit', 'derived_field': 'existing_value'}
    ]

    # Run the pipeline and expect a ValueError
    with pytest.raises(ValueError, match="Target column 'derived_field' already exists in the element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Apply GenericDeriveCondition' >> beam.ParDo(
                GenericDeriveCondition(
                    column='field1',
                    map={'apple': 'sweet', 'carrot': 'healthy'},
                    new_column='derived_field'
                )
            )