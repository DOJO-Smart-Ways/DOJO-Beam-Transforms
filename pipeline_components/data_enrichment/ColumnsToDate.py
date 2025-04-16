import apache_beam as beam

class ColumnsToDate(beam.DoFn):
    """
    A generic Apache Beam DoFn class for converting date formats within elements of a PCollection.

    This class takes a list of column names, a list of possible input date formats, and the desired output date format.
    It also handles timezone-aware conversions if the input dates include timezone information.
    """

    def __init__(self, columns, input_formats, output_format="%Y-%m-%d", timezone=None):
        """
        Initializes the ColumnsToDate class with specific formatting details.

        Args:
            columns (list of str): A list of column names containing date strings to be converted.
            input_formats (list of str): A list of strftime-compatible formatting strings for input dates.
            output_format (str): The strftime-compatible formatting string for output dates (default: "%Y-%m-%d").
            timezone (str): The timezone to apply to the converted dates (optional).
        """
        self.columns = columns
        self.input_formats = input_formats
        self.output_format = output_format
        self.timezone = timezone

    def process(self, element):
        from datetime import datetime
        import pytz

        for column in self.columns:
            if column not in element:
                raise KeyError(f"Column '{column}' not found in the input element: {element}")

            date_str = element[column]
            date_obj = None

            # Try each input format until one works
            for input_format in self.input_formats:
                try:
                    date_obj = datetime.strptime(date_str, input_format)
                    break
                except ValueError:
                    continue

            # If no format matched, raise an error
            if date_obj is None:
                raise ValueError(f"Error: None of the input formats matched for column '{column}' with value '{date_str}'")

            # Apply timezone if specified
            if self.timezone:
                tz = pytz.timezone(self.timezone)
                date_obj = tz.localize(date_obj)

            # Format the date object to the desired output format
            element[column] = date_obj.strftime(self.output_format)

        yield element