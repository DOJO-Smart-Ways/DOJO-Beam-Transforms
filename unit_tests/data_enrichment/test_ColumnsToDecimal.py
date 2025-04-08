import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from decimal import Decimal
from pipeline_components.data_enrichment.ColumnsToDecimal import ColumnsToDecimal

@pytest.mark.ColumnsToDecimal
def test_columns_to_decimal_converter():
    # Input data
    input_data = [
        {'price': '12.34', 'quantity': '5'},
        {'price': 'invalid', 'quantity': '10'},
        {'price': None, 'quantity': '3.5'},
        {'price': '45.67', 'quantity': None}
    ]

    # Expected output
    expected_output = [
        {'price': Decimal('12.34'), 'quantity': Decimal('5')},
        {"error": "Error converting column 'price' to Decimal", "element": {'price': 'invalid', 'quantity': '10'}},
        {"error": "Error converting column 'price' to Decimal", "element": {'price': None, 'quantity': '3.5'}},
        {'price': Decimal('45.67'), 'quantity': None}
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert to Decimal' >> beam.ParDo(ColumnsToDecimal(columns=['price', 'quantity']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ColumnsToDecimal
def test_columns_to_decimal_converter_error():
    # Input data with an invalid value
    input_data = [
        {'price': 'invalid', 'quantity': '10'}
    ]

    # Run the pipeline and expect an error
    with pytest.raises(ValueError, match="Error converting column 'price' to Decimal"):
        with TestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            output_pcoll = input_pcoll | 'Convert to Decimal' >> beam.ParDo(ColumnsToDecimal(columns=['price']))