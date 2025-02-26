import apache_beam as beam

class ReplaceValues(beam.DoFn):
    """
    ReplaceValues is used to replace specific values within specified columns of a PCollection's element 
    (expected to be a dictionary). The replacements are defined in a list of tuples, where each tuple contains 
    the column name, the value to replace, and the new value.
    """
    def __init__(self, replacements):
        """
        Initializes the ReplaceValues instance.
        
        Args:
            replacements (list of tuples): A list where each tuple contains (column name, current value, replacement value).
        """
        self.replacements = replacements

    def process(self, element):
        """
        Processes each element, replacing specified values within specified columns.
        
        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column, current_value, replacement in self.replacements:
            if column in element:
                column_value = element[column]
                if isinstance(column_value, str) and current_value in column_value:
                    element[column] = column_value.replace(current_value, replacement)
        yield element
