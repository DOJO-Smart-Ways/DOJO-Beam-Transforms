import apache_beam as beam

class DropColumns(beam.DoFn):
    """
    A DoFn for dropping specified columns from each element in a PCollection.
    
    This function is useful in scenarios where certain columns of data are not needed
    for further processing or analysis, allowing for the reduction of data volume and
    simplification of the data structure.
    
    Attributes:
        column (list of str): A list containing the names of the columns to be dropped.
    """
    
    def __init__(self, column):
        """
        Initializes the DropColumns instance with the names of columns to drop.
        
        Args:
            column (list of str): The names of the columns to be dropped from each element.
                                  Can be a single string if only one column needs to be dropped.
        """
        # Ensure column is a list to simplify processing
        self.column = column if isinstance(column, list) else [column]

    def process(self, element):
        """
        Processes each element in the PCollection, dropping specified columns.
        
        Args:
            element (dict): An element of the PCollection, expected to be a dictionary
                            from which specified columns will be removed.
                            
        Yields:
            The modified element with specified columns removed or an error if a column is missing.
        """
        try:
            for col in self.column:
                if col not in element:
                    raise ValueError(f"Column '{col}' not found in element: {element}")
                element.pop(col)
            yield element
        except ValueError as e:
            yield {"error": str(e)}

