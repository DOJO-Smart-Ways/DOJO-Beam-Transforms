import apache_beam as beam

class DeriveSingleValue(beam.DoFn):
    """
    A class that extends DoFn to derive a single value and add it as a new column 
    to each element in the PCollection.

    Parameters:
    - value: The single value to be assigned to the new column.
    - new_column: The name of the new column where the value will be inserted.
    """
    def __init__(self, value, new_column):
        self.value = value
        self.new_column = new_column

    def process(self, element):
        # Assign the single value to the new column
        element[self.new_column] = self.value
        yield element
