import apache_beam as beam

class TrimValues(beam.DoFn):
    def __init__(self, columns):
        """
        Initializes the TrimValues instance.

        Args:
            columns (list of str): A list of column names to trim spaces from.
        """
        self.columns = columns

    def process(self, element):
        """
        Processes each element, trimming leading and trailing spaces from specified columns.

        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column in self.columns:
            if column in element and isinstance(element[column], str):
                element[column] = element[column].strip()
        yield element
