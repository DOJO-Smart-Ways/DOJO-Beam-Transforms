import pytest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment.ColumnValueAssignment import ColumnValueAssignment

@pytest.mark.ColumnValueAssignment
def test_column_value_assignment():
    # Input data
    input_data = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'},
        {'id': 3, 'name': 'Charlie'}
    ]

    # Expected output
    expected_output = [
        {'id': 1, 'name': 'Alice', 'status': 'active'},
        {'id': 2, 'name': 'Bob', 'status': 'active'},
        {'id': 3, 'name': 'Charlie', 'status': 'active'}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Assign Value' >> beam.ParDo(ColumnValueAssignment(value='active', new_column='status'))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ColumnValueAssignment
def test_column_value_assignment_overwrite():
    input_data = [
        {'id': 1, 'name': 'Alice', 'status': 'inactive'},
        {'id': 2, 'name': 'Bob', 'status': 'inactive'}
    ]

    # Run the pipeline and expect a ValueError
    with pytest.raises(ValueError, match=r"Error: Column 'status' already exists in element: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Assign Value' >> beam.ParDo(ColumnValueAssignment(value='active', new_column='status'))


@pytest.mark.ColumnValueAssignment
def test_column_value_assignment_invalid_value_type():
    # Tenta criar uma instância com um tipo de valor inválido
    with pytest.raises(TypeError, match=r"Value '123' does not match the expected type str"):
        ColumnValueAssignment(value=123, new_column='status', value_type=str)


@pytest.mark.ColumnValueAssignment
def test_column_value_assignment_non_dict_element():
    # Input data contendo um elemento que não é um dicionário
    input_data = [
        ['not', 'a', 'dictionary'],  # Elemento inválido
        {'id': 1, 'name': 'Alice'}  # Elemento válido
    ]

    # Run the pipeline and expect a ValueError
    with pytest.raises(ValueError, match=r"Error: element is not a dictionary: .*"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Assign Value' >> beam.ParDo(ColumnValueAssignment(value='active', new_column='status'))