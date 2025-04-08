import apache_beam as beam

class ColumnValueAssignment(beam.DoFn):
    """
    A class that extends DoFn to derive a single value and add it as a new column 
    to each element in the PCollection.

    Parameters:
    - value: The single value to be assigned to the new column.
    - new_column: The name of the new column where the value will be inserted.
    - value_type: The expected type of the value (e.g., str, int, float). If provided, 
                  the value will be validated against this type.
    """
    def __init__(self, value, new_column, value_type=None):
        self.value = value
        self.new_column = new_column
        self.value_type = value_type

        # Validate the value type if specified
        if self.value_type and not isinstance(self.value, self.value_type):
            raise TypeError(f"Value '{self.value}' does not match the expected type {self.value_type.__name__}")

    def process(self, element):
        # Check if the element is a dictionary
        if not isinstance(element, dict):
            yield {"error": f"Element is not a dictionary: {element}"}
            

        # Check if the new column already exists
        if self.new_column in element:
            yield {"error": f"Column '{self.new_column}' already exists in element: {element}"}
            return

        # Assign the single value to the new column
        element[self.new_column] = self.value
        yield element
