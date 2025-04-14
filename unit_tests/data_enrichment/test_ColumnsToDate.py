import pytest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_enrichment.ColumnsToDate import ColumnsToDate

@pytest.mark.ColumnsToDate
def test_columns_to_date_conversion():
    # Input data
    input_data = [
        {'date1': '2023-10-01', 'date2': '01/10/2023'},
        {'date1': '2023-09-15', 'date2': '15/09/2023'}
    ]

    # Expected output
    expected_output = [
        {'date1': '2023-10-01', 'date2': '2023-10-01'},
        {'date1': '2023-09-15', 'date2': '2023-09-15'}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert Dates' >> beam.ParDo(
            ColumnsToDate(columns=['date1', 'date2'], input_formats=['%Y-%m-%d', '%d/%m/%Y'], output_format='%Y-%m-%d')
        )

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ColumnsToDate
def test_columns_to_date_invalid_format():
    # Input data
    input_data = [
        {'date1': 'invalid-date', 'date2': '01/10/2023'}
    ]

    # Run the pipeline and expect a ValueError
    with pytest.raises(ValueError, match="Error: None of the input formats matched for column 'date1' with value 'invalid-date'"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Convert Dates' >> beam.ParDo(
                ColumnsToDate(columns=['date1', 'date2'], input_formats=['%Y-%m-%d', '%d/%m/%Y'])
            )

@pytest.mark.ColumnsToDate
def test_columns_to_date_missing_column():
    # Input data
    input_data = [
        {'date1': '2023-10-01'},
        {'date1': '2023-09-15'}
    ]

    # Run the pipeline and expect a KeyError
    with pytest.raises(KeyError, match="Column 'date2' not found in the input element"):
        with BeamTestPipeline() as p:
            input_pcoll = p | 'Create Input' >> beam.Create(input_data)
            _ = input_pcoll | 'Convert Dates' >> beam.ParDo(
                ColumnsToDate(columns=['date1', 'date2'], input_formats=['%Y-%m-%d', '%d/%m/%Y'])
            )

@pytest.mark.ColumnsToDate
def test_columns_to_date_multiple_formats():
    # Input data
    input_data = [
        {'date1': '2023-10-01', 'date2': '01/10/2023'},
        {'date1': '2023-09-15 12:00:00', 'date2': '15/09/2023'}
    ]

    # Expected output
    expected_output = [
        {'date1': '2023-10-01', 'date2': '2023-10-01'},
        {'date1': '2023-09-15', 'date2': '2023-09-15'}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Convert Dates' >> beam.ParDo(
            ColumnsToDate(
                columns=['date1', 'date2'],
                input_formats=['%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S'],
                output_format='%Y-%m-%d'
            )
        )

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))