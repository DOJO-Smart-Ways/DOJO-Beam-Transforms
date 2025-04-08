import apache_beam as beam

class GenericDeriveCondition(beam.DoFn):
    """
    A DoFn that derives a new column value based on a mapping of existing column values.

    Attributes:
        column (str): The name of the column to check for categories.
        map (dict): A dictionary mapping values to descriptions.
        new_column (str): The name of the new column that satisfies the map.
        default (str): The default description if the column does not exist or the value is not in the map.

    Example:
        Input:
            element = {'category': 'A', 'value': 100}
            column = 'category'
            map = {'A': 'Type 1', 'B': 'Type 2'}
            new_column = 'category_description'
            default = 'Unknown'

        Output:
            element = {'category': 'A', 'value': 100, 'category_description': 'Type 1'}
    """
    def __init__(self, column, map, new_column, default='UNKNOWN'):
        """
        Initializes the DoFn with the necessary parameters.

        Args:
            column (str): The name of the column to check for categories.
            map (dict): A dictionary mapping values to descriptions.
            new_column (str): The name of the new column that satisfies the map.
            default (str): The default description if the column does not exist or the value is not in the map.
        """
        self.column = column
        self.map = map
        self.new_column = new_column
        self.default = default

    def process(self, element):
        """
        Processes each element, deriving a new column value based on the mapping.

        Args:
            element (dict): The input element to process.

        Yields:
            dict: The modified element with the new column added or updated.
        """
        try:
            # Check if the specified column exists in the element
            if self.column in element:
                # Get the value from the element
                value = element[self.column]

                # Map the value to the corresponding description
                if value in self.map:
                    element[self.new_column] = self.map[value]
                else:
                    element[self.new_column] = self.map.get('default', self.default)
            else:
                # If the column does not exist, set the description to the default
                element[self.new_column] = self.default
        except Exception as e:
            raise ValueError(f"Error processing element {element}: {e}")

        yield element