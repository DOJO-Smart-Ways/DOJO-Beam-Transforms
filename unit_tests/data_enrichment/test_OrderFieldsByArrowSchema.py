import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment.OrderFieldsByArrowSchema import OrderFieldsByArrowSchema
import pyarrow as pa

@pytest.mark.OrderFieldsByArrowSchema
def test_order_fields_by_arrow_schema():
    # Define Arrow schema
    arrow_schema = pa.schema([
        ('field1', pa.string()),
        ('field2', pa.int32()),
        ('field3', pa.float64())
    ])
    
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
        output_pcoll = input_pcoll | 'Apply OrderFieldsByArrowSchema' >> beam.ParDo(OrderFieldsByArrowSchema(arrow_schema))
        assert_that(output_pcoll, equal_to(expected_output))
