from decimal import Decimal
import apache_beam as beam

class ColumnsToInteger(beam.DoFn):
    def __init__(self, columns):
        """
        Initializes the ColumnsToInteger instance.

        Args:
            columns (list of str): A list of column names whose values should be converted to integers.
        """
        # Validate that columns is a list
        if not isinstance(columns, list):
            raise TypeError(f"Columns must be a list, but got {type(columns).__name__}.")

        # Validate that all elements in the list are strings
        if not all(isinstance(col, str) for col in columns):
            raise ValueError("All columns must be strings.")

        self.columns = columns

    def process(self, element):
        """
        Processes each element, converting specified column values to integer.

        Args:
            element (dict): The input element to process, where keys are column names.
        """
        # Validate input
        if not isinstance(element, dict):
            raise ValueError("Input element must be a dictionary.")

        for column in self.columns:
            if column not in element:
                raise KeyError(f"Column '{column}' not found in the input element: {element}")

            # Skip conversion if the value is None
            if element[column] is None:
                continue

            if isinstance(element[column], (str, float, Decimal)):
                try:
                    element[column] = int(float(element[column]))
                except (ValueError, TypeError):
                    raise ValueError(f"Error converting column '{column}' to Integer. Element: {element}")

        yield element