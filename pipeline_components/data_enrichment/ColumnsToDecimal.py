from decimal import Decimal, InvalidOperation
import apache_beam as beam

class ColumnsToDecimal(beam.DoFn):
    """
    Converts specified columns in the input data to decimal type.

    This transform ensures that the specified columns are converted to Decimal, raising errors for invalid conversions.

    Attributes:
        columns (list): List of column names to convert to Decimal.

    Example:
        Input: {'price': '12.34', 'quantity': '5'}
        Transform: ColumnsToDecimal(columns=['price', 'quantity'])
        Output: {'price': Decimal('12.34'), 'quantity': Decimal('5')}
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
                    element[column] = Decimal(element[column])
                except (InvalidOperation, ValueError) as e:
                    yield {"error": f"Error converting column '{column}' to Decimal: {e}", "element": element}
                    return

        yield element