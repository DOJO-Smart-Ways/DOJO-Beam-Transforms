import apache_beam as beam

class GenericDeriveConditionComplex(beam.DoFn):
    """
    A DoFn that derives a new column value based on complex conditions applied to existing column values.

    Attributes:
        conditions (list): A list of dictionaries, each containing a condition (lambda function) and a value to assign.
        new_column (str): The name of the new column to be added or updated based on the conditions.
        default (str or callable): The default value to assign if none of the conditions match. Can be a static value or a callable.
    
    Example:
        Input:
            element = {'COLUMN_1': 'D', 'COLUMN_2': '0040.2'}
            conditions = [
                {
                    'condition': lambda x: (x['COLUMN_1'] == 'D' and x['COLUMN_2'] in ['0040.2', '0053']),
                    'value': 'MATCHED'
                },
                {
                    'condition': lambda x: x['COLUMN_1'] == 'M',
                    'value': 'MISMATCHED'
                }
            ]
            new_column = 'RESULT'
            default = 'NO_MATCH'

        Output:
            element = {'COLUMN_1': 'D', 'COLUMN_2': '0040.2', 'RESULT': 'MATCHED'}
    """
    def __init__(self, conditions, new_column, default='UNKNOWN'):
        """
        Initializes the DoFn with complex conditions.

        Args:
            conditions (list): A list of dictionaries, each representing a condition (as a lambda function) and its corresponding value to assign.
            new_column (str): The name of the new column to be added or updated based on the conditions.
            default (str or callable): The default value to assign if none of the conditions match.
        """
        self.conditions = conditions
        self.new_column = new_column
        self.default = default

    def process(self, element):
        """
        Processes each element, evaluating conditions and assigning values to the new column.

        Args:
            element (dict): The input element to process.

        Yields:
            dict: The modified element with the new column added or updated.
        """
        # Check if the target column already exists
        if self.new_column in element:
            raise ValueError(f"Target column '{self.new_column}' already exists in the element: {element}")

        try:
            # Evaluate each condition against the element
            for condition in self.conditions:
                if condition['condition'](element):
                    # If the condition is true, assign the corresponding value and break
                    element[self.new_column] = condition['value']
                    break
            else:
                # If no condition matched, assign the default value
                if callable(self.default):
                    element[self.new_column] = self.default(element)
                else:
                    element[self.new_column] = self.default
        except Exception as e:
            raise ValueError(f"Error processing element {element}: {e}")

        yield element