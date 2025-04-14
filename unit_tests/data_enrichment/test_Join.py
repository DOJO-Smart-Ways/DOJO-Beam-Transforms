import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline_components.data_enrichment.Join import Join

@pytest.mark.Join
def test_left_join():
    # Input data for the first PCollection (TABLE1)
    table1_data = [
        {'id': 1, 'city': 'New York', 'population': 8000000},
        {'id': 2, 'city': 'Los Angeles', 'population': 4000000},
        {'id': 3, 'city': 'Austin', 'population': 2700000},
    ]

    # Input data for the second PCollection (TABLE2)
    table2_data = [
        {'city': 'New York', 'state': 'NY'},
        {'city': 'Los Angeles', 'state': 'CA'},
    ]

    # Expected output after the LeftJoin
    expected_output = [
        {'id': 1, 'city': 'New York', 'population': 8000000, 'city_2': 'New York', 'state': 'NY'},
        {'id': 2, 'city': 'Los Angeles', 'population': 4000000, 'city_2': 'Los Angeles', 'state': 'CA'},
        {'id': 3, 'city': 'Austin', 'population': 2700000, 'city_2': None, 'state': None}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        table1_pcoll = p | 'Create TABLE1' >> beam.Create(table1_data)
        table2_pcoll = p | 'Create TABLE2' >> beam.Create(table2_data)

        result_pcoll = table1_pcoll | 'Apply LeftJoin' >> Join(join_table=table2_pcoll, keys=[('city', 'city')], join_type='left')

        assert_that(result_pcoll, equal_to(expected_output))


@pytest.mark.Join
def test_inner_join():
    # Input data for the first PCollection (TABLE1)
    table1_data = [
        {'id': 1, 'city': 'New York', 'population': 8000000},
        {'id': 2, 'city': 'Los Angeles', 'population': 4000000},
    ]

    # Input data for the second PCollection (TABLE2)
    table2_data = [
        {'city': 'New York', 'state': 'NY'},
        {'city': 'Los Angeles', 'state': 'CA'},
        {'city': 'Austin', 'state': 'TX'},
    ]

    # Expected output after the InnerJoin
    expected_output = [
        {'id': 1, 'city': 'New York', 'population': 8000000, 'city_2': 'New York', 'state': 'NY'},
        {'id': 2, 'city': 'Los Angeles', 'population': 4000000, 'city_2': 'Los Angeles', 'state': 'CA'},
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        table1_pcoll = p | 'Create TABLE1' >> beam.Create(table1_data)
        table2_pcoll = p | 'Create TABLE2' >> beam.Create(table2_data)

        result_pcoll = table1_pcoll | 'Apply InnerJoin' >> Join(join_table=table2_pcoll, keys=[('city', 'city')], join_type='inner')

        assert_that(result_pcoll, equal_to(expected_output))


@pytest.mark.Join
def test_right_join():
    # Input data for the first PCollection (TABLE1)
    table1_data = [
        {'id': 1, 'city': 'New York', 'population': 8000000},
    ]

    # Input data for the second PCollection (TABLE2)
    table2_data = [
        {'city': 'New York', 'state': 'NY'},
        {'city': 'Los Angeles', 'state': 'CA'},
    ]

    # Expected output after the RightJoin
    expected_output = [
        {'city_2': 'New York', 'state': 'NY', 'id': 1, 'city': 'New York', 'population': 8000000},
        {'city_2': 'Los Angeles', 'state': 'CA', 'id': None, 'city': None, 'population': None},
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        table1_pcoll = p | 'Create TABLE1' >> beam.Create(table1_data)
        table2_pcoll = p | 'Create TABLE2' >> beam.Create(table2_data)

        result_pcoll = table1_pcoll | 'Apply RightJoin' >> Join(join_table=table2_pcoll, keys=[('city', 'city')], join_type='right')

        assert_that(result_pcoll, equal_to(expected_output))


@pytest.mark.Join
def test_invalid_join_type():
    with pytest.raises(ValueError, match="Unsupported join_type 'invalid', supported types are: \['left', 'inner', 'right'\]"):
        with BeamTestPipeline() as p:
            table1_pcoll = p | 'Create TABLE1' >> beam.Create([{'id': 1, 'city': 'New York'}])
            table2_pcoll = p | 'Create TABLE2' >> beam.Create([{'city': 'New York', 'state': 'NY'}])
            _ = table1_pcoll | 'Apply InvalidJoinType' >> Join(join_table=table2_pcoll, keys=[('city', 'city')], join_type='invalid')


@pytest.mark.Join
def test_invalid_keys():
    with pytest.raises(ValueError, match="Keys must be a non-empty list of tuples with two elements each."):
        with BeamTestPipeline() as p:
            table1_pcoll = p | 'Create TABLE1' >> beam.Create([{'id': 1, 'city': 'New York'}])
            table2_pcoll = p | 'Create TABLE2' >> beam.Create([{'city': 'New York', 'state': 'NY'}])
            _ = table1_pcoll | 'Apply InvalidKeys' >> Join(join_table=table2_pcoll, keys=['city'], join_type='left')