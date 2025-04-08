import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing import TestPipeline
from pipeline_components.data_enrichment.ColumnsToFloat import ColumnsToFloat

@pytest.mark.ColumnsToFloat
def test_columns_to_float():
    # Input data
    input_data = [
        {'price': '12.34', 'quantity': '5'},
        {'price': 'invalid', 'quantity': '10'},
        {'price': None, 'quantity': '3.5'},
        {'price': '45.67', 'quantity': None}
    ]

    # Expected output
    expected_output = [
        {'price': 12.34, 'quantity': 5.0},
        {"error": "Error converting column 'price' to float", "element": {'price': 'invalid', 'quantity': '10'}},
        {"error": "Error converting column 'price' to float", "element": {'price': None, 'quantity': '3.5'}},
        {'price': 45.67, 'quantity': None}
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert to Float' >> beam.ParDo(ColumnsToFloat(columns=['price', 'quantity']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))