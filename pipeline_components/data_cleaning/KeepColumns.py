import apache_beam as beam

class KeepColumns(beam.DoFn):
    """
    A DoFn for keeping specified columns from each element in a PCollection.

    This function is useful in scenarios where certain columns of data are needed
    for further processing or analysis, allowing for the reduction of data volume.

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
        # Ensure columns is a list to simplify processing
        self.columns = columns if isinstance(columns, list) else [columns]

    def process(self, element):
        """
        Processes each element in the PCollection, keeping only specified columns.

        Args:
            element (dict): An element of the PCollection, expected to be a dictionary
                            with keys corresponding to column names.

        Yields:
            A dictionary containing only the specified columns to be kept.
        """
        # Create a new dictionary with only the columns to keep
        result = {col: element[col] for col in self.columns if col in element}
        yield result
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
        # Ensure columns is a list to simplify processing
        self.columns = columns if isinstance(columns, list) else [columns]

    def process(self, element):
        """
        Processes each element in the PCollection, keeping only specified columns.

        Args:
            element (dict): An element of the PCollection, expected to be a dictionary
                            with keys corresponding to column names.

        Yields:
            A dictionary containing only the specified columns to be kept.
        """
        # Create a new dictionary with only the columns to keep
        result = {col: element[col] for col in self.columns if col in element}
        yield result
