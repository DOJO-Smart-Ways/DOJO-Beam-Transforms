import apache_beam as beam

class ColumnsToFloat(beam.DoFn):
    """
    Converts specified columns in the input data to float type.

    This transform ensures that the specified columns are converted to float, raising errors for invalid conversions.

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
            if column in element:
                try:
                    element[column] = float(element[column])
                except (ValueError, TypeError) as e:
                    yield {"error": f"Error converting column '{column}' to float: {e}", "element": element}
                    return

        yield element