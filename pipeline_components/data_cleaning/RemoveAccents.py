import apache_beam as beam

class RemoveAccents(beam.DoFn):
    """
    A class to remove accents from specified columns of elements.

    Attributes:
        columns (list): A list of column names from which accents will be removed.
    """

    def __init__(self, columns):
        """
        Initializes the RemoveAccents object with the specified columns.

        Args:
            columns (list): A list of column names from which accents will be removed.
        """
        self.columns = columns

    def process(self, element):
        """
        Processes each element in the pipeline, removing accents from specified columns.

        Args:
            element (dict): The element to be processed.

        Returns:
            list: A list containing the processed element with accents removed.
        """
        from unidecode import unidecode
        try:
            for col in self.columns:
                if col in element and element[col] is not None:
                    element[col] = unidecode(str(element[col]))
        except Exception as e:
            print(f"Error while removing accents: {e}")
        return [element]
