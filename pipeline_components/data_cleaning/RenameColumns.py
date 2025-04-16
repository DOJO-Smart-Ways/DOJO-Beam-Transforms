import apache_beam as beam

class RenameColumns(beam.DoFn):
    """
    RenameColumns is designed to rename columns in a PCollection's element (expected to be a dictionary). 
    The renaming is defined by a column_mapping dictionary where keys are original column names, and 
    values are the new names for these columns.
    """
    def __init__(self, column_mapping):
        """
        Initializes the RenameColumns instance.
        
        Args:
            column_mapping (dict): A dictionary mapping from old column names to new column names where keys are actual columns names and values are the new expected column name.
        """
        # Validate column_mapping
        if not isinstance(column_mapping, dict):
            raise TypeError(f"column_mapping must be a dictionary, but got {type(column_mapping).__name__}.")
        if not all(isinstance(key, str) and isinstance(value, str) for key, value in column_mapping.items()):
            raise ValueError("All keys and values in column_mapping must be strings.")
        self.column_mapping = column_mapping

    def process(self, element):
        """
        Processes each element, renaming columns as specified in column_mapping.
        
        Args:
            element (dict): The input element to process, where keys are column names.
        """
        new_element = {self.column_mapping.get(k, k): v for k, v in element.items()}
        yield new_element
