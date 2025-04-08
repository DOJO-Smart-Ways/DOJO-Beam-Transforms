import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import pipeline_components.data_enrichment as de 

@pytest.mark.LeftJoin
def test_left_join():
    # Input data for the first PCollection (TABLE1)
    table1_data = [
        {'id': 1, 'name': 'Alice', 'age': 25},
        {'id': 2, 'name': 'Bob', 'age': 30},
        {'id': 3, 'name': 'Charlie', 'age': 35}
    ]

    # Input data for the second PCollection (TABLE2)
    table2_data = [
        {'id': 1, 'city': 'New York'},
        {'id': 2, 'city': 'Los Angeles'}
    ]

    # Expected output after the LeftJoin
    expected_output = [
        {'id': 1, 'name': 'Alice', 'age': 25, 'city': 'New York'},
        {'id': 2, 'name': 'Bob', 'age': 30, 'city': 'Los Angeles'},
        {'id': 3, 'name': 'Charlie', 'age': 35, 'city': ''}
    ]

    # Run the pipeline
    with TestPipeline() as p:
        # Create PCollections for TABLE1 and TABLE2
        table1_pcoll = p | 'Create TABLE1' >> beam.Create(table1_data)
        table2_pcoll = p | 'Create TABLE2' >> beam.Create(table2_data)

        # Apply transformations and LeftJoin
        result_pcoll = (
            table1_pcoll
            | 'Convert TABLE1 Columns to String' >> beam.ParDo(de.ColumnsToString(columns=['name']))
            | 'Apply LeftJoinFn' >> de.Join(keys=[('city', 'cidade')], table2=table2_pcoll, join_type='left')
        )

        # Assert the output
        assert_that(result_pcoll, equal_to(expected_output))