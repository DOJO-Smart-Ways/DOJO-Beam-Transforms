import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline_components.data_cleaning.HandleNaNValues import HandleNaNValues
import csv

def read_csv(file_path):
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]
        for row in data:
            for key, value in row.items():
                try:
                    row[key] = float(value)
                except ValueError:
                    pass
        return data

def test_replace_nan_with_default():
    input_data = read_csv('/workspaces/DOJO-Beam-Transforms/data/california_housing.csv')
    input_data[0]['median_house_value'] = float('nan')
    input_data[2]['median_house_value'] = float('nan')

    expected_output = input_data.copy()
    expected_output[0]['median_house_value'] = 0
    expected_output[2]['median_house_value'] = 0

    with TestPipeline() as p:
        result = (
            p
            | 'Create Input' >> beam.Create(input_data)
            | 'Handle NaN' >> beam.ParDo(HandleNaNValues(strategy='replace', default_value=0))
        )

        assert_that(result, equal_to(expected_output))

def test_remove_rows_with_nan():
    input_data = read_csv('/workspaces/DOJO-Beam-Transforms/data/california_housing.csv')
    input_data[0]['median_house_value'] = float('nan')
    input_data[2]['median_house_value'] = float('nan')

    expected_output = input_data[1:2]

    with TestPipeline() as p:
        result = (
            p
            | 'Create Input' >> beam.Create(input_data)
            | 'Handle NaN' >> beam.ParDo(HandleNaNValues(strategy='remove'))
        )

        assert_that(result, equal_to(expected_output))

def test_replace_nan_in_specific_columns():
    input_data = read_csv('/workspaces/DOJO-Beam-Transforms/data/california_housing.csv')
    input_data[0]['median_house_value'] = float('nan')
    input_data[2]['median_house_value'] = float('nan')

    expected_output = input_data.copy()
    expected_output[0]['median_house_value'] = 0
    expected_output[2]['median_house_value'] = 0

    with TestPipeline() as p:
        result = (
            p
            | 'Create Input' >> beam.Create(input_data)
            | 'Handle NaN' >> beam.ParDo(HandleNaNValues(strategy='replace', default_value=0, columns=['median_house_value']))
        )

        assert_that(result, equal_to(expected_output))
