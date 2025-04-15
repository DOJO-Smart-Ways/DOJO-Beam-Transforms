import pytest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment import GenericArithmeticOperation

def test_generic_arithmetic_operation():
    # Define input data
    input_data = [
        {'field1': 10, 'field2': 20},
        {'field1': 5, 'field2': 15},
    ]

    # Define expected output
    expected_output = [
        {'field1': 10, 'field2': 20, 'result': 30, 'result_2': 200, 'result_3': -10, 'result_4': 0.5},
        {'field1': 5, 'field2': 15, 'result': 20, 'result_2': 75, 'result_3': -10, 'result_4': 0.3333333333333333},
    ]

    params = [
        {
            'operands': ['field1', 'field2'],   
            'result_column': 'result',
            'formula': lambda x, y: x+y
        },
        {
            'operands': ['field1', 'field2'],   
            'result_column': 'result_2',
            'formula': lambda x, y: x*y
        },
        {
            'operands': ['field1', 'field2'],   
            'result_column': 'result_3',
            'formula': lambda x, y: x-y
        },
        {
            'operands': ['field1', 'field2'],   
            'result_column': 'result_4',
            'formula': lambda x, y: x/y
        }
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply GenericArithmeticOperation' >> beam.ParDo(
            GenericArithmeticOperation(params),
        )
        assert_that(output_pcoll, equal_to(expected_output))


@pytest.mark.GenericArithmeticOperation
def test_missing_column_in_operands():
    # Input data
    input_data = [
        {'COLUMN_1': 10, 'COLUMN_2': 20}  # Missing 'COLUMN_3'
    ]

    # Define operations
    operations = [
        {
            'operands': ['COLUMN_1', 'COLUMN_2', 'COLUMN_3'],  # 'COLUMN_3' is missing
            'result_column': 'COLUMN_4',
            'formula': lambda c1, c2, c3: (c1 + c2) / c3 if c3 else 0
        }
    ]

    # Run the pipeline and expect a KeyError
    with pytest.raises(KeyError, match="Missing column in operation: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Apply Operation' >> beam.ParDo(GenericArithmeticOperation(operations))


@pytest.mark.GenericArithmeticOperation
def test_target_column_already_exists():
    # Input data
    input_data = [
        {'COLUMN_1': 10, 'COLUMN_2': 20, 'COLUMN_3': 5, 'COLUMN_4': 100}  # 'COLUMN_4' already exists
    ]

    # Define operations
    operations = [
        {
            'operands': ['COLUMN_1', 'COLUMN_2', 'COLUMN_3'],
            'result_column': 'COLUMN_4',  # Target column already exists
            'formula': lambda c1, c2, c3: (c1 + c2) / c3 if c3 else 0
        }
    ]

    # Run the pipeline and expect a ValueError
    with pytest.raises(ValueError, match="Target column 'COLUMN_4' already exists in the element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Apply Operation' >> beam.ParDo(GenericArithmeticOperation(operations))