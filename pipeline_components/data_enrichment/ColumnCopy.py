import apache_beam as beam

class ColumnCopy(beam.DoFn):
    """
    A DoFn that copies the value of one column to another column in the input data.

    This transform is useful when duplicating data for further processing or creating backup columns.

    Attributes:
        source_column (str): The name of the column to copy data from.
        target_column (str): The name of the column to copy data to.

    Example:
        Input: {'name': 'Alice', 'age': 30}
        Transform: ColumnCopy(source_column='age', target_column='age_backup')
        Output: {'name': 'Alice', 'age': 30, 'age_backup': 30}
    """
    def __init__(self, source_column, target_column):
        """
        Initializes the ColumnCopy.

        Args:
            source_column (str): The name of the column to copy from.
            target_column (str): The name of the column to paste into.
        """
        self.copy_column_name = source_column
        self.paste_column_name = target_column

    def process(self, element):
        """
        Copies the value of one column to another column in the given element.

        Args:
            element (dict): A dictionary representing a row, with column names as keys and values as values.

        Yields:
            dict: A dictionary representing the modified row after copying the column value.
        """
        # Validate input column names
        if self.copy_column_name not in element:
            raise ValueError(f"Column '{self.copy_column_name}' not found in the input data.")
        if self.paste_column_name in element:
            raise ValueError(f"Column '{self.paste_column_name}' already exists in the input data.")

        # Copy the column value
        element[self.paste_column_name] = element[self.copy_column_name]
        
        yield element