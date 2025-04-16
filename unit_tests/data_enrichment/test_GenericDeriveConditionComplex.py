import pytest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment import GenericDeriveConditionComplex

@pytest.mark.GenericDeriveConditionComplex
def test_generic_derive_condition_complex():
    # Define input data
    input_data = [
        {'field1': 'apple', 'field2': 'fruit'},
        {'field1': 'carrot', 'field2': 'vegetable'},
        {'field1': 'stone', 'field2': 'mineral'},
    ]

    # Define expected output
    expected_output = [
        {'field1': 'apple', 'field2': 'fruit', 'derived_field': 'edible'},
        {'field1': 'carrot', 'field2': 'vegetable', 'derived_field': 'edible'},
        {'field1': 'stone', 'field2': 'mineral', 'derived_field': 'non-edible'},
    ]

    # Define conditions
    conditions = [
        {'condition': lambda x: x['field2'] in ['fruit', 'vegetable'], 'value': 'edible'},
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply GenericDeriveConditionComplex' >> beam.ParDo(
            GenericDeriveConditionComplex(conditions, new_column='derived_field', default='non-edible')
        )
        assert_that(output_pcoll, equal_to(expected_output))


@pytest.mark.GenericDeriveConditionComplex
def test_generic_derive_condition_complex_target_column_already_exists():
    # Input data
    input_data = [
        {'field1': 'apple', 'field2': 'fruit', 'derived_field': 'existing_value'}
    ]

    # Define conditions
    conditions = [
        {'condition': lambda x: x['field2'] in ['fruit', 'vegetable'], 'value': 'edible'},
    ]

    # Run the pipeline and expect a ValueError
    with pytest.raises(ValueError, match="Target column 'derived_field' already exists in the element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Apply GenericDeriveConditionComplex' >> beam.ParDo(
                GenericDeriveConditionComplex(conditions, new_column='derived_field', default='non-edible')
            )


@pytest.mark.GenericDeriveConditionComplex
def test_generic_derive_condition_complex_error_in_condition():
    # Input data
    input_data = [
        {'field1': 'apple', 'field2': 'fruit'}
    ]

    # Define conditions
    conditions = [
        {'condition': lambda x: x['non_existent_field'] == 'value', 'value': 'error'},  # This will raise a KeyError
    ]

    # Run the pipeline and expect a ValueError
    with pytest.raises(ValueError, match="Error processing element .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Apply GenericDeriveConditionComplex' >> beam.ParDo(
                GenericDeriveConditionComplex(conditions, new_column='derived_field', default='non-edible')
            )