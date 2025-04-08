from decimal import Decimal
import apache_beam as beam

class ColumnsToInteger(beam.DoFn):
    def __init__(self, columns):
        """
        Initializes the ColumnsToInteger instance.

        Args:
            columns (list of str): A list of column names whose values should be converted to integers.
        """
        self.columns = columns

    def process(self, element):
        """
        Processes each element, converting specified column values to integer.

        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column in self.columns:
            if column not in element:
                raise KeyError(f"Column '{column}' not found in the input element: {element}")

            if isinstance(element[column], (str, float, Decimal)):
                try:
                    element[column] = int(element[column])
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Error converting column '{column}' to Integer: {e}")
        yield element