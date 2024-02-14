import apache_beam as beam

#################################################################################################################
# RenameColumns is designed to rename columns in a PCollection's element (expected to be a dictionary). 
# The renaming is defined by a column_mapping dictionary where keys are original column names, and 
# values are the new names for these columns.
#################################################################################################################
class RenameColumns(beam.DoFn):
    def __init__(self, column_mapping):
        """
        Initializes the RenameColumns instance.
        
        Args:
            column_mapping (dict): A dictionary mapping from old column names to new column names.
        """
        self.column_mapping = column_mapping

    def process(self, element):
        """
        Processes each element, renaming columns as specified in column_mapping.
        
        Args:
            element (dict): The input element to process, where keys are column names.
        """
        new_element = {self.column_mapping.get(k, k): v for k, v in element.items()}
        yield new_element

#################################################################################################################
# ReplaceValues is used to replace specific values within specified columns of a PCollection's element 
# (expected to be a dictionary). The replacements are defined in a list of tuples, where each tuple contains 
# the column name, the value to replace, and the new value.
#################################################################################################################
class ReplaceValues(beam.DoFn):
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

#################################################################################################################
# ColumnsToStringConverter ensures that values in specified columns of a PCollection's element 
# (expected to be a dictionary) are converted to strings. This is particularly useful for ensuring data 
# type consistency before writing to file systems or databases that require string inputs.
#################################################################################################################
class ColumnsToStringConverter(beam.DoFn):
    def __init__(self, columns_to_string):
        """
        Initializes the ColumnsToStringConverter instance.
        
        Args:
            columns_to_string (list of str): A list of column names whose values should be converted to strings.
        """
        self.columns_to_string = columns_to_string

    def process(self, element):
        """
        Processes each element, converting specified column values to strings.
        
        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column in self.columns_to_string:
            if column in element:
                try:
                    if element[column] is None or element[column] == "":
                        element[column] = " "
                    else:
                        element[column] = str(element[column])
                except (ValueError, TypeError):
                    pass
        yield element
