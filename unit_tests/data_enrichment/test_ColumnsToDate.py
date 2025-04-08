import pytest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from pipeline_components.data_enrichment.ColumnsToDate import ColumnsToDate
from datetime import datetime

@pytest.mark.ColumnsToDate
def test_columns_to_date_conversion():
    # Input data
    input_data = [
        {'date1': '2023-10-01', 'date2': '01/10/2023'},
        {'date1': '2023-09-15', 'date2': '15/09/2023'}
    ]

    # Expected output
    expected_output = [
        {'date1': datetime(2023, 10, 1), 'date2': datetime(2023, 10, 1)},
        {'date1': datetime(2023, 9, 15), 'date2': datetime(2023, 9, 15)}
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert Dates' >> beam.ParDo(ColumnsToDate(columns=['date1', 'date2'], date_formats=['%Y-%m-%d', '%d/%m/%Y']))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ColumnsToDate
def test_columns_to_date_missing_column():
    # Input data
    input_data = [
        {'date1': '2023-10-01'},
        {'date1': '2023-09-15'}
    ]

    # Run the pipeline and expect a KeyError
    with pytest.raises(KeyError, match="Column 'date2' not found in the input element"):
        with TestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            output_pcoll = input_pcoll | 'Convert Dates' >> beam.ParDo(ColumnsToDate(columns=['date1', 'date2'], date_formats=['%Y-%m-%d', '%d/%m/%Y']))

@pytest.mark.ColumnsToDate
def test_columns_to_date_invalid_format():
    # Input data
    input_data = [
        {'date1': 'invalid-date', 'date2': '01/10/2023'}
    ]

    # Run the pipeline and expect a ValueError
    with pytest.raises(ValueError, match="Invalid date format in column 'date1'"):
        with TestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            output_pcoll = input_pcoll | 'Convert Dates' >> beam.ParDo(ColumnsToDate(columns=['date1', 'date2'], date_formats=['%Y-%m-%d', '%d/%m/%Y']))

@pytest.mark.ColumnsToDate
def test_columns_to_date_with_timezone():
    # Input data
    input_data = [
        {'date1': '2023-10-01 12:00:00', 'date2': '2023-10-01 15:00:00'},
        {'date1': '2023-09-15 08:30:00', 'date2': '2023-09-15 11:30:00'}
    ]

    # Expected output
    expected_output = [
        {'date1': '2023-10-01T12:00:00+03:00', 'date2': '2023-10-01T15:00:00+03:00'},
        {'date1': '2023-09-15T08:30:00+03:00', 'date2': '2023-09-15T11:30:00+03:00'}
    ]

    # Run the pipeline
    with TestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert Dates with Timezone' >> beam.ParDo(
            ColumnsToDate(
                columns=['date1', 'date2'],
                input_format='%Y-%m-%d %H:%M:%S',
                output_format='%Y-%m-%dT%H:%M:%S%z',
                timezone='Europe/Moscow'
            )
        )

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))