import apache_beam as beam

class ColumnsToDate(beam.DoFn):
    """
    A generic Apache Beam DoFn class for converting date formats within elements of a PCollection.

    This class takes a list of column names, the expected input date format, and the desired output date format.
    It also handles timezone-aware conversions if the input dates include timezone information.
    """

    def __init__(self, columns, input_format, output_format="%Y-%m-%d", timezone=None):
        """
        Initializes the ColumnsToDate class with specific formatting details.

        Args:
            columns (list of str): A list of column names containing date strings to be converted.
            input_format (str): The strftime-compatible formatting string for input dates.
            output_format (str): The strftime-compatible formatting string for output dates (default: "%Y-%m-%d").
            timezone (str): The timezone to apply to the converted dates (optional).
        """
        self.columns = columns
        self.input_format = input_format
        self.output_format = output_format
        self.timezone = timezone

    def process(self, element):
        from datetime import datetime
        import pytz

        for column in self.columns:
            if column not in element:
                raise KeyError(f"Column '{column}' not found in the input element: {element}")

            date_str = element[column]
            try:
                # Parse the date string using the input format
                date_obj = datetime.strptime(date_str, self.input_format)

                # Apply timezone if specified
                if self.timezone:
                    tz = pytz.timezone(self.timezone)
                    date_obj = tz.localize(date_obj)

                # Format the date object to the desired output format
                element[column] = date_obj.strftime(self.output_format)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Error processing column '{column}' with value '{date_str}': {e}")

        yield element