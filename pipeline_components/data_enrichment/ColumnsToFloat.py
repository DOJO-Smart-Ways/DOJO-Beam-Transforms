import apache_beam as beam

class ColumnsToFloat(beam.DoFn):
    """
    Converts specified columns in the input data to float type.

    This transform ensures that the specified columns are converted to float, handling None values gracefully
    and raising errors for invalid conversions.

    Attributes:
        columns (list): List of column names to convert to float.

    Example:
        Input: {'price': '12.34', 'quantity': '5'}
        Transform: ColumnsToFloat(columns=['price', 'quantity'])
        Output: {'price': 12.34, 'quantity': 5.0}
    """
    def __init__(self, columns):
        self.columns = columns

    def process(self, element):
        # Validate input
        if not isinstance(element, dict):
            raise ValueError("Input element must be a dictionary.")

        for column in self.columns:
            if column not in element:
                raise KeyError(f"Column '{column}' not found in the input element: {element}")

            # Skip conversion if the value is None
            if element[column] is None:
                continue

            try:
                element[column] = float(element[column])
            except (ValueError, TypeError):
                raise ValueError(f"Error converting on column '{column}' to Float. Element {element}")

        yield element