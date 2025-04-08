import apache_beam as beam

class GenericArithmeticOperation(beam.DoFn):
    """
    A Beam DoFn class for performing generic operations on columns of a PCollection.
    Operations include arithmetic calculations, replacing missing values, and type conversion.
    """
    
    def __init__(self, operations):
        """
        Initializes the class with a list of operations to be performed.
        operations = [
            {
            'operands': ['COLUMN_1', 'COLUMN_2', 'COLUMN_3'],
            'result_column': 'COLUMN_4',
            'formula': lambda c1, c2, c3: (c1 + c2) / c3 if c3 else 0  # Avoid division by zero
            }
        ]
        """
        self.operations = operations

    def process(self, element):
        """
        Processes each element based on the specified operations.
        """
        for operation in self.operations:
            # Perform arithmetic operation
            operands = operation.get('operands', [])
            result_column = operation.get('result_column')
            formula = operation.get('formula', lambda x: x)  # A lambda function to apply on operands
            
            # Gather operand values, default to 0 if not found or None
            try:
                values = [element[col] for col in operands]
            except KeyError as e:
                raise KeyError(f"Missing column in operation: {e}")

            # Apply formula and update element with result
            try:
                element[result_column] = formula(*values)
            except Exception as e:
                raise ValueError(f"Error applying formula for result column '{result_column}': {e}")

        yield element