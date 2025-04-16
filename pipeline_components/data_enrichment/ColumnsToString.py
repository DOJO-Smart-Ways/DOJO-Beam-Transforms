import apache_beam as beam

class ColumnsToString(beam.DoFn):
    def __init__(self, columns):
        """
        Initializes the ColumnsToString instance.
        
        Args:
            columns (list of str): A list of column names whose values should be converted to strings.
        """
        self.columns = columns

    def process(self, element):
        """
        Processes each element, converting specified column values to strings.
        
        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column in self.columns:
            if column not in element:
                raise KeyError(f"Column '{column}' not found in the input element: {element}")
            
            try:
                # Convert the value to a string if it's not None
                element[column] = str(element[column]) if element[column] is not None else None
            except (ValueError, TypeError) as e:
                raise ValueError(f"Error converting column '{column}' to string: {e}")
        yield element