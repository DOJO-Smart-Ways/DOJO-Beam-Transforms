import apache_beam as beam

class TrimValues(beam.DoFn):
    """
    A class that extends DoFn to trim whitespace from the values of specified columns 
    for each element in the PCollection.

    Parameters:
    - columns: A list of column names where the values will be trimmed.
    """
    def __init__(self, columns):
        # Validate columns
        if not isinstance(columns, list):
            raise TypeError(f"Columns must be a list, but got {type(columns).__name__}.")
        if not all(isinstance(col, str) for col in columns):
            raise ValueError("All columns must be strings.")
        self.columns = columns

    def process(self, element):
        """
        Processes each element, trimming leading and trailing spaces from specified columns.

        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column in self.columns:
            try:
                if column not in element:
                    raise ValueError(f"Column '{column}' not found in element: {element}")
                if not isinstance(element[column], str):
                    raise TypeError(f"Column '{column}' value is not a string: {element}")
                element[column] = element[column].strip()
            except (ValueError, TypeError) as e:
                yield {"error": str(e)}
                return
        yield element
