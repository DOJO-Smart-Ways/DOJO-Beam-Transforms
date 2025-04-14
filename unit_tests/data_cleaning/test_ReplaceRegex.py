import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from pipeline_components.data_cleaning.ReplaceRegex import ReplaceRegex

@pytest.mark.ReplaceRegex
def test_replace_regex():
    # Define input data
    input_data = [
        {'city_name': 'New York123'},
        {'city_name': 'Los Angeles456'}
    ]
    
    # Define regex patterns to replace (example: remove digits and extra spaces)
    patterns = [(r'\d+', ''), (r'\s{2,}', '')]

    # Define expected output (regex applied to 'city_name')
    expected_output = [
        {'city_name': 'New York'},
        {'city_name': 'Los Angeles'}
    ]

    # Run the pipeline
    with BeamTestPipeline() as p:
        input_pcoll = p | 'Create Input' >> beam.Create(input_data)
        output_pcoll = input_pcoll | 'Apply ReplaceRegex' >> beam.ParDo(ReplaceRegex(columns=['city_name'], patterns=patterns))

        # Assert the output
        assert_that(output_pcoll, equal_to(expected_output))

@pytest.mark.ReplaceRegex
def test_replace_regex_invalid_pattern():    
    # Define an invalid regex pattern
    patterns = [(r'[', '')]  # Invalid regex pattern

    # Expect the test to raise a ValueError due to invalid regex
    with pytest.raises(ValueError, match="Invalid regex pattern: \\["):
        ReplaceRegex(columns=['city_name'], patterns=patterns)

@pytest.mark.ReplaceRegex
def test_replace_regex_invalid_columns_type():
    # Expect the test to raise a TypeError due to invalid columns type
    with pytest.raises(TypeError, match="Columns must be a list, but got str."):
        ReplaceRegex(columns="invalid_column", patterns=[(r'\d+', '')])

@pytest.mark.ReplaceRegex
def test_replace_regex_invalid_columns_content():
    # Expect the test to raise a ValueError due to non-string column in the list
    with pytest.raises(ValueError, match="All columns must be strings."):
        ReplaceRegex(columns=["valid_column", 123], patterns=[(r'\d+', '')])

@pytest.mark.ReplaceRegex
def test_replace_regex_invalid_patterns_type():
    # Expect the test to raise a TypeError due to invalid patterns type
    with pytest.raises(TypeError, match="Patterns must be a list, but got str."):
        ReplaceRegex(columns=['city_name'], patterns="invalid_pattern")

@pytest.mark.ReplaceRegex
def test_replace_regex_invalid_patterns_content():
    # Expect the test to raise a ValueError due to invalid pattern content
    with pytest.raises(ValueError, match="Each pattern must be a tuple with a regex pattern and a replacement value."):
        ReplaceRegex(columns=['city_name'], patterns=[("valid_pattern",), "invalid_pattern"])
