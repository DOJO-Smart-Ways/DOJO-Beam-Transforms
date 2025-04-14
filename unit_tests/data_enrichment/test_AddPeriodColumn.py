import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment.AddPeriodColumn import AddPeriodColumn
from datetime import datetime

def test_add_period_column():
    # Define input data
    input_data = [
        {'date': datetime(2025, 4, 10), 'value': 100},
        {'date': datetime(2025, 2, 15), 'value': 200},
    ]

    # Define expected output
    expected_output = [
        {'date': datetime(2025, 4, 10), 'value': 100, 'period_end': '2025-04-30'},
        {'date': datetime(2025, 2, 15), 'value': 200, 'period_end': '2025-02-28'},
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply AddPeriodColumn' >> beam.ParDo(
            AddPeriodColumn(),
            input_column='date',
            output_column='period_end',
            date_format='%Y-%m-%d'
        )
        assert_that(output_pcoll, equal_to(expected_output))