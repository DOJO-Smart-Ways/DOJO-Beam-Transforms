import apache_beam as beam

class KeepColumns(beam.DoFn):
    """
    A DoFn for keeping specified columns from each element in a PCollection.

    This function is useful in scenarios where certain columns of data are needed
    for further processing or analysis, allowing for the reduction of data volume and
    simplification of the data structure.

    Attributes:
        columns (list of str): A list containing the names of the columns to be kept.
    """

    def __init__(self, columns):
        """
        Initializes the KeepColumns instance with the names of columns to keep.

        Args:
            columns (list of str): The names of the columns to be kept from each element.
                                   Can be a single string if only one column needs to be kept.
        """
        # Validate columns
        if not isinstance(columns, list):
            raise TypeError(f"Columns must be a list, but got {type(columns).__name__}.")
        if not all(isinstance(col, str) for col in columns):
            raise ValueError("All columns must be strings.")
        self.columns = columns

    def process(self, element):
        """
        Processes each element in the PCollection, keeping only specified columns.

        Args:
            element (dict): An element of the PCollection, expected to be a dictionary
                            with keys corresponding to column names.

        Yields:
            A dictionary containing only the specified columns to be kept.
        """
        try:
            # Keep only the specified columns
            filtered_element = {key: value for key, value in element.items() if key in self.columns}
            yield filtered_element
        except Exception as e:
            yield {"error": str(e)}
