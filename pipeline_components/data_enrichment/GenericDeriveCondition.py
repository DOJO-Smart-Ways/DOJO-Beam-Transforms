import apache_beam as beam

class GenericDeriveCondition(beam.DoFn):
    """
    A Beam DoFn class for deriving a new column based on conditions applied to an existing column.
    """
    
    def __init__(self, column, map, new_column):
        """
        Initializes the class with the column to derive from, the mapping, and the new column name.
        
        Args:
            column (str): The column to apply the mapping to.
            map (dict): A dictionary mapping input values to derived values.
            new_column (str): The name of the new column to create.
        """
        self.column = column
        self.map = map
        self.new_column = new_column

    def process(self, element):
        """
        Processes each element to derive a new column based on the mapping.
        """
        # Check if the source column exists
        if self.column not in element:
            raise KeyError(f"Source column '{self.column}' not found in the element: {element}")
        
        # Check if the target column already exists
        if self.new_column in element:
            raise ValueError(f"Target column '{self.new_column}' already exists in the element: {element}")
        
        # Apply the mapping
        value = element[self.column]
        if value in self.map:
            element[self.new_column] = self.map[value]
        else:
            element[self.new_column] = 'default'  # Default value if no match is found
        
        yield element