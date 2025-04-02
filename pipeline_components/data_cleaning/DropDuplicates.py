import apache_beam as beam

class DropDuplicates(beam.DoFn):
    """
    A DoFn for removing duplicate elements from a PCollection based on specific keys (columns) or the entire element.

    Parameters:
    - columns: A list of column names to consider for deduplication. If None, the entire element will be used.
    """
    def __init__(self, columns=None):
        self.columns = columns if columns else []

        # Validate columns during initialization
        if not isinstance(self.columns, list):
            raise TypeError(f"Columns must be a list, but got {type(self.columns).__name__}")
        if not all(isinstance(col, str) for col in self.columns):
            raise ValueError("All columns must be strings.")

    def process(self, element, seen=set()):
        try:
            # Check if element is a dictionary
            if not isinstance(element, dict):
                raise TypeError(f"Element is not a dictionary: {element}")

            # Generate a deduplication key
            if self.columns:
                for col in self.columns:
                    if col not in element:
                        raise ValueError(f"Column '{col}' not found in element: {element}")
                dedup_key = tuple(element[col] for col in self.columns)
            else:
                dedup_key = tuple(element.items())

            # Check if the deduplication key has already been seen
            if dedup_key in seen:
                return
            seen.add(dedup_key)

            yield element

        except (TypeError, ValueError) as e:
            yield {"error": str(e)}
