import apache_beam as beam
import unicodedata

class RemoveAccents(beam.DoFn):
    """
    A DoFn for removing accents from specified columns in each element of a PCollection.

    Parameters:
    - columns: A list of column names where accents will be removed.
    """
    def __init__(self, columns):
        # Validate columns
        if not isinstance(columns, list):
            raise TypeError(f"Columns must be a list, but got {type(columns).__name__}.")
        if not all(isinstance(col, str) for col in columns):
            raise ValueError("All columns must be strings.")
        self.columns = columns

    def process(self, element):
        # Remove accents from the specified columns
        for column in self.columns:
            try:
                if column not in element:
                    raise ValueError(f"Column '{column}' not found in element: {element}")
                if element[column] and not isinstance(element[column], str):
                    raise TypeError(f"Column '{column}' value is not a string: {element}")
                element[column] = ''.join(
                    c for c in unicodedata.normalize('NFD', element[column]) if unicodedata.category(c) != 'Mn'
                ) if element[column] else None
            except (ValueError, TypeError) as e:
                yield {"error": str(e)}
                return
        yield element
