class ColumnsToLowerCase(beam.DoFn):
    """
    A DoFn that converts specified columns in a dictionary element to lowercase.

    Attributes:
        columns (list): List of column names to convert to lowercase.
    """
    def __init__(self, columns):
        """
        Initializes the ColumnsToLowerCase instance.

        Args:
            columns (list of str): A list of column names to convert to lowercase.
        """
        self.columns = columns

    def process(self, element):
        """
        Processes each element, converting specified column values to lowercase.

        Args:
            element (dict): The input element to process, where keys are column names.

        Yields:
            dict: The modified element with specified columns converted to lowercase.
        """
        for column in self.columns:
            if column not in element:
                raise KeyError(f"Column '{column}' not found in the input element: {element}")

            try:
                # Convert the value to lowercase if it's a string
                if isinstance(element[column], str):
                    element[column] = element[column].lower()
                else:
                    raise ValueError(f"Value in column '{column}' is not a string: {element[column]}")
            except Exception as e:
                raise ValueError(f"Error processing column '{column}' with value '{element[column]}': {e}")

        yield element