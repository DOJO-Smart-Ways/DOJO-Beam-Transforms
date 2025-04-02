import apache_beam as beam

class ReplaceValues(beam.DoFn):
    """
    A class that extends DoFn to replace specific values in the specified columns 
    for each element in the PCollection.

    Parameters:
    - columns: A list of column names where the values will be replaced.
    - replacements: A dictionary where keys are the values to search for and 
                    values are the replacements.
    """
    def __init__(self, columns, replacements):
        # Validate columns
        if not isinstance(columns, list):
            raise TypeError(f"Columns must be a list, but got {type(columns).__name__}.")
        if not all(isinstance(col, str) for col in columns):
            raise ValueError("All columns must be strings.")
        self.columns = columns

        # Validate replacements
        if not isinstance(replacements, dict):
            raise TypeError(f"Replacements must be a dictionary, but got {type(replacements).__name__}.")
        self.replacements = replacements

    def process(self, element):
        # Replace values in the specified columns
        for column in self.columns:
            try:
                if column in element:
                    for target_value, replacement_value in self.replacements.items():
                        if element[column] == target_value:
                            element[column] = replacement_value
                else:
                    raise ValueError(f"Column '{column}' not found in element: {element}")
            except ValueError as e:
                yield {"error": str(e)}
                return
        yield element
