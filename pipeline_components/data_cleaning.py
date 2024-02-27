import apache_beam as beam

class HandleNaN(beam.DoFn):
    """
    Treatment of NaN values ​​in the column
    """
    def process(self, element):
        import math
        for key, value in element.items():
            if isinstance(value, list):
                # Replace nan in lists
                element[key] = ["" if isinstance(item, float) and math.isnan(item) else item for item in value]
            elif value is None or (isinstance(value, float) and math.isnan(value)):
                # Replace single nan values
                element[key] = ''

        yield element

class FilterColumnValues(beam.DoFn):
    """
    A DoFn that filters elements based on specified values in a given column.
    
    This function allows for selective filtering of records in a PCollection where
    only elements with a specific column containing a value from a predefined list 
    are retained. This is particularly useful for data cleaning and preprocessing,
    where you may want to exclude records based on certain criteria.
    
    Attributes:
        column (str): The name of the column to check for specified values.
        values_to_filter (list of str): A list of string values. If the value in the
                                        specified column matches any value in this list,
                                        the element will be filtered out.
    """
    
    def __init__(self, column, values_to_filter):
        """
        Initializes the FilterColumnValues instance with the column name and values to filter.
        
        Args:
            column (str): The name of the column to check against the values_to_filter.
            values_to_filter (list of str): The list of values to filter by; elements with these
                                            values in the specified column will be excluded.
        """
        self.column = column
        self.values_to_filter = values_to_filter

    def process(self, element):
        """
        Processes each element in the PCollection, yielding those that do not match the filter criteria.
        
        Args:
            element (dict): An element of the PCollection, expected to be a dictionary with keys
                            corresponding to column names.
                            
        Yields:
            The element itself if its value in the specified column does not match any of the
            values in the values_to_filter list; otherwise, nothing is yielded for that element.
        """
        # Check if the element's value for the specified column is not in the list of values to filter
        if element.get(self.column) not in self.values_to_filter:
            yield element
            

class DropColumns(beam.DoFn):
    """
    A DoFn for dropping specified columns from each element in a PCollection.
    
    This function is useful in scenarios where certain columns of data are not needed
    for further processing or analysis, allowing for the reduction of data volume and
    simplification of the data structure.
    
    Attributes:
        column (list of str): A list containing the names of the columns to be dropped.
    """
    
    def __init__(self, column):
        """
        Initializes the DropColumns instance with the names of columns to drop.
        
        Args:
            column (list of str): The names of the columns to be dropped from each element.
                                  Can be a single string if only one column needs to be dropped.
        """
        # Ensure column is a list to simplify processing
        self.column = column if isinstance(column, list) else [column]

    def process(self, element):
        """
        Processes each element in the PCollection, dropping specified columns.
        
        Args:
            element (dict): An element of the PCollection, expected to be a dictionary
                            from which specified columns will be removed.
                            
        Yields:
            The modified element with specified columns removed.
        """
        # Iterate over the list of columns to drop and remove them from the element
        for col in self.column:
            element.pop(col, None)  # Use pop with None as default to avoid KeyError if the column is missing
        yield element

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
