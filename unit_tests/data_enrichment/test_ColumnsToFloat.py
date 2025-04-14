import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment.ColumnsToFloat import ColumnsToFloat


@pytest.mark.ColumnsToFloat
def test_columns_to_float_valid_and_null_values():
    # Input data
    input_data = [
        {'price': '12.34', 'quantity': '5'},  # Conversão válida
        {'price': None, 'quantity': '3.5'},  # Valor nulo para 'price'
        {'price': '45.67', 'quantity': None}  # Valor nulo para 'quantity'
    ]

    # Expected output
    expected_output = [
        {'price': 12.34, 'quantity': 5.0},
        {'price': None, 'quantity': 3.5},
        {'price': 45.67, 'quantity': None}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert to Float' >> beam.ParDo(ColumnsToFloat(columns=['price', 'quantity']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))


@pytest.mark.ColumnsToFloat
def test_columns_to_float_invalid_value():
    # Input data
    input_data = [
        {'price': 'invalid', 'quantity': '10'}  # Valor inválido para 'price'
    ]

    # Run the pipeline and expect a ValueError
    with pytest.raises(ValueError, match=r"Error converting on column 'price' to Float. Element .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Convert to Float' >> beam.ParDo(ColumnsToFloat(columns=['price', 'quantity']))


@pytest.mark.ColumnsToFloat
def test_columns_to_float_missing_column():
    # Input data
    input_data = [
        {'price': '12.34'}  # Falta a coluna 'quantity'
    ]

    # Run the pipeline and expect a KeyError
    with pytest.raises(KeyError, match=r"Column 'quantity' not found in the input element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Convert to Float' >> beam.ParDo(ColumnsToFloat(columns=['price', 'quantity']))