import pytest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
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
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Assign Value' >> beam.ParDo(ColumnValueAssignment(column='status', value='active'))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ColumnValueAssignment
def test_column_value_assignment_overwrite():
    # Input data
    input_data = [
        {'id': 1, 'name': 'Alice', 'status': 'inactive'},
        {'id': 2, 'name': 'Bob', 'status': 'inactive'}
    ]

    # Expected output
    expected_output = [
        {'id': 1, 'name': 'Alice', 'status': 'active'},
        {'id': 2, 'name': 'Bob', 'status': 'active'}
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Assign Value' >> beam.ParDo(ColumnValueAssignment(column='status', value='active'))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ColumnValueAssignment
def test_column_value_assignment_missing_column():
    # Input data
    input_data = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'}
    ]

    # Expected output
    expected_output = [
        {'id': 1, 'name': 'Alice', 'status': 'active'},
        {'id': 2, 'name': 'Bob', 'status': 'active'}
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Assign Value' >> beam.ParDo(ColumnValueAssignment(column='status', value='active'))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))