import apache_beam as beam

class ReplaceMissingValues(beam.DoFn):
    """
    A DoFn that replaces missing or invalid values in specified columns with predefined replacement values.

    Attributes:
        replacements (dict): A dictionary where keys are column names and values are the replacement values for missing entries.
    """
    def __init__(self, replacements):
        """
        Initializes the ReplaceMissingValues instance.

        Args:
            replacements (dict): A dictionary where keys are column names and values are the replacement values for missing entries.
        """
        self.replacements = replacements

    def process(self, element):
        """
        Processes each element, replacing missing or invalid values in the specified columns.

        Args:
            element (dict): The input element to process, where keys are column names.

        Yields:
            dict: The modified element with missing or invalid values replaced.
        """
        try:
            for column, replacement_value in self.replacements.items():
                if column not in element or element[column] in [None, '', float('nan')]:
                    element[column] = replacement_value
        except Exception as e:
            raise ValueError(f"Error processing element {element}: {e}")

        yield element