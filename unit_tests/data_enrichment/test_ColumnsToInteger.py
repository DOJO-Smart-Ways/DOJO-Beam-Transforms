import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment.ColumnsToInteger import ColumnsToInteger

@pytest.mark.ColumnsToInteger
def test_columns_to_integer_error():
    # Input data with invalid values
    input_data = [
        {'age': 'invalid', 'score': '50'},
        {'age': '', 'score': None}
    ]

    # Run the pipeline and expect an error
    with pytest.raises(ValueError, match="Error converting column 'age' to Integer"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            output_pcoll = input_pcoll | 'Convert to Integer' >> beam.ParDo(ColumnsToInteger(columns=['age']))


@pytest.mark.ColumnsToInteger
def test_columns_to_integer_invalid_columns_type():
    # Expect the test to raise a TypeError due to invalid columns type
    with pytest.raises(TypeError, match="Columns must be a list, but got str."):
        ColumnsToInteger(columns="invalid_column")


@pytest.mark.ColumnsToInteger
def test_columns_to_integer_invalid_columns_content():
    # Expect the test to raise a ValueError due to non-string column in the list
    with pytest.raises(ValueError, match="All columns must be strings."):
        ColumnsToInteger(columns=["valid_column", 123])


@pytest.mark.ColumnsToInteger
def test_columns_to_integer_missing_column():
    # Input data
    input_data = [
        {'price': '12'}  # Falta a coluna 'quantity'
    ]

    # Run the pipeline and expect a KeyError
    with pytest.raises(KeyError, match=r"Column 'quantity' not found in the input element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Convert to Integer' >> beam.ParDo(ColumnsToInteger(columns=['price', 'quantity']))



@pytest.mark.ColumnsToInteger
def test_columns_to_integer_valid_and_null_values():
    # Input data
    input_data = [
        {'price': '12', 'quantity': 5.0},  # Conversão válida
        {'price': None, 'quantity': '3.0'},  # Valor nulo para 'price'
        {'price': '45', 'quantity': None}  # Valor nulo para 'quantity'
    ]

    # Expected output
    expected_output = [
        {'price': 12, 'quantity': 5},
        {'price': None, 'quantity': 3},
        {'price': 45, 'quantity': None}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert to Integer' >> beam.ParDo(ColumnsToInteger(columns=['price', 'quantity']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))