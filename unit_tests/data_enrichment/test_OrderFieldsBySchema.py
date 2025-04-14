import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment.OrderFieldsBySchema import OrderFieldsBySchema

@pytest.mark.OrderFieldsBySchema
def test_order_fields_by_schema():
    # Define schema string
    schema_str = """
    field1: STRING
    field2: INTEGER
    field3: FLOAT
    """
    
    # Define input data
    input_data = [
        {'field1': 'value1', 'field2': 123, 'field3': 45.6},
        {'field1': 'value2', 'field3': 78.9},  # Missing 'field2'
        {'field2': 456},  # Missing 'field1' and 'field3'
    ]
    
    # Define expected output
    expected_output = [
        {'field1': 'value1', 'field2': 123, 'field3': 45.6},
        {'field1': 'value2', 'field2': None, 'field3': 78.9},
        {'field1': None, 'field2': 456, 'field3': None},
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply OrderFieldsBySchema' >> beam.ParDo(OrderFieldsBySchema(schema_str))
        assert_that(output_pcoll, equal_to(expected_output))
