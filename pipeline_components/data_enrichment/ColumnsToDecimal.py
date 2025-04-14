from decimal import Decimal, InvalidOperation
import apache_beam as beam

class ColumnsToDecimal(beam.DoFn):
    """
    Converts specified columns in the input data to decimal type.

    This transform ensures that the specified columns are converted to Decimal, handling None values gracefully
    and raising errors for invalid conversions.

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
            if column not in element:
                raise KeyError(f"Column '{column}' not found in the input element: {element}")

            # Skip conversion if the value is None
            if element[column] is None:
                continue

            try:
                element[column] = Decimal(element[column])
            except (InvalidOperation, ValueError):
                raise ValueError(f"Error converting on column '{column}' to Decimal. Element {element}")
                

        yield element