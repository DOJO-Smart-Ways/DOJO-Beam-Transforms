class ColumnsToUpperCase(beam.DoFn):
    """
    A DoFn that converts specified columns in a dictionary element to uppercase.

    Attributes:
        columns (list): List of column names to convert to uppercase.
    """
    def __init__(self, columns):
        """
        Initializes the ColumnsToUpperCase instance.

        Args:
            columns (list of str): A list of column names to convert to uppercase.
        """
        self.columns = columns

    def process(self, element):
        """
        Processes each element, converting specified column values to uppercase.

        Args:
            element (dict): The input element to process, where keys are column names.

        Yields:
            dict: The modified element with specified columns converted to uppercase.
        """
        for column in self.columns:
            if column not in element:
                raise KeyError(f"Column '{column}' not found in the input element: {element}")

            try:
                # Convert the value to uppercase if it's a string
                if isinstance(element[column], str):
                    element[column] = element[column].upper()
                else:
                    raise ValueError(f"Value in column '{column}' is not a string: {element[column]}")
            except Exception as e:
                raise ValueError(f"Error processing column '{column}' with value '{element[column]}': {e}")

        yield element