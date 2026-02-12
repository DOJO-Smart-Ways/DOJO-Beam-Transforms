import pytest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment import ColumnValueAssignment

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
def test_column_value_assignment_overwrite_dofn_direct():
    input_element = {'id': 1, 'name': 'Alice', 'status': 'inactive'}
    
    # Instancia o DoFn diretamente
    dofn = ColumnValueAssignment(value='active', new_column='status')
    
    # Simula o método process. Como é um generator, usamos list() para forçar a execução
    with pytest.raises(ValueError, match=r"Error: Column 'status' already exists in element: .*"):
        list(dofn.process(input_element))


@pytest.mark.ColumnValueAssignment
def test_column_value_assignment_invalid_value_type():
    # Tenta criar uma instância com um tipo de valor inválido
    with pytest.raises(TypeError, match=r"Value '123' does not match the expected type str"):
        ColumnValueAssignment(value=123, new_column='status', value_type=str)


@pytest.mark.ColumnValueAssignment
def test_column_value_assignment_non_dict_element():
    # Input data inválido (lista em vez de dict)
    element_invalido = ['not', 'a', 'dictionary']

    # 1. Instancie sua classe DoFn diretamente
    dofn = ColumnValueAssignment(value='active', new_column='status')

    # 2. Execute o método process() manualmente.
    # Usamos list() porque o process retorna um generator (yield) e precisamos consumi-lo para o erro estourar.
    with pytest.raises(ValueError, match=r"Error: element is not a dictionary: .*"):
        list(dofn.process(element_invalido))