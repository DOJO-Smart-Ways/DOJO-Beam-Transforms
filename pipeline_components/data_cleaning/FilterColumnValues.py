import apache_beam as beam

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
import apache_beam as beam

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
import apache_beam as beam

class DCFilterColumnValuesFn(beam.DoFn):
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
        Initializes the DCFilterColumnValuesFn instance with the column name and values to filter.
        
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
import apache_beam as beam

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
