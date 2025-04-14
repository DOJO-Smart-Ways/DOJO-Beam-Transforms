import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from decimal import Decimal
from pipeline_components.data_enrichment.ColumnsToDecimal import ColumnsToDecimal

@pytest.mark.ColumnsToDecimal
def test_columns_to_decimal_converter():
    # Input data
    input_data = [
        {'price': '12.34', 'quantity': '5'},  # Conversão válida
        {'price': '0', 'quantity': '10'},  # Valor inválido para 'price'
        {'price': None, 'quantity': '3.5'},  # Valor nulo para 'price'
        {'price': '45.67', 'quantity': None}  # Valor nulo para 'quantity'
    ]

    # Expected output
    expected_output = [
        {'price': Decimal('12.34'), 'quantity': Decimal('5')},
        {'price': Decimal('0'), 'quantity': Decimal('10')},  # Mantém 'price' como None
        {'price': None, 'quantity': Decimal('3.5')},  # Mantém 'price' como None
        {'price': Decimal('45.67'), 'quantity': None}  # Mantém 'quantity' como None
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert to Decimal' >> beam.ParDo(ColumnsToDecimal(columns=['price', 'quantity']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ColumnsToDecimal
def test_columns_to_decimal_missing_column():
    # Input data
    input_data = [
        {'price': '12.34'}  # Falta a coluna 'quantity'
    ]

    # Run the pipeline and expect a KeyError
    with pytest.raises(KeyError, match="Column 'quantity' not found in the input element"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Convert to Decimal' >> beam.ParDo(ColumnsToDecimal(columns=['price', 'quantity']))

@pytest.mark.ColumnsToDecimal
def test_columns_to_decimal_invalid_value():
    # Input data
    input_data = [
        {'price': 'invalid', 'quantity': '10'}  # Valor inválido para 'price'
    ]
    
    # Run the pipeline and expect a ValueError
    with pytest.raises(ValueError, match=r"Error converting on column 'price' to Decimal. Element .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Convert to Decimal' >> beam.ParDo(ColumnsToDecimal(columns=['price', 'quantity']))

   