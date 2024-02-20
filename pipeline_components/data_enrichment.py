import apache_beam as beam


class ConvertToUpperCase(beam.DoFn):
    """
    A DoFn that converts all string values in a dictionary element to uppercase.
    
    This function iterates over each key-value pair in the input element. If a value
    is of type string, it converts the string to uppercase. This is useful in data 
    normalization processes where consistent case formatting is required.
    
    The process method is designed to handle any exceptions that occur during the 
    conversion, logging an error message for debugging purposes. This ensures that 
    the pipeline can continue processing other elements even if one element causes 
    an issue.
    """
    
    def process(self, element):
        import logging
        """
        Processes each element in the PCollection, converting all string values to uppercase.
        
        Args:
            element (dict): An element of the PCollection, expected to be a dictionary.
            
        Yields:
            The modified element with all string values converted to uppercase.
        """
        # Create a copy of the keys to iterate over to avoid modifying the dictionary while iterating
        keys = list(element.keys())
        
        for key in keys:
            value = element[key]
            # Check if the value is a string
            try:
                if isinstance(value, str):
                    # Convert the string to uppercase and update the element
                    element[key] = value.upper()
            except Exception as e:
                # Log the error for debugging purposes
                logging.error(f"Error processing key {key} with value {value}: {str(e)}")
        
        yield element

class KeyByComposite(beam.DoFn):
    """
    Transforms input elements into key-value pairs where the key is a composite made from 
    specified columns of the input element, and the value is the entire input element. 
    Useful for grouping or joining data based on multiple columns.
    """
    def __init__(self, key_columns):
        """
        Initializes the KeyByComposite instance.
        
        Args:
            key_columns (list of str): Column names to be used for forming the composite key.
        """
        self.key_columns = key_columns

    def process(self, element):
        """
        For each input element, creates a composite key and yields a tuple of this key 
        and the original element.
        
        Args:
            element (dict): The input element to process.
        """
        composite_key = {k:element[k] for k in self.key_columns if k in element}
        grouped_data = {k:element[k] for k in element}
        yield (composite_key, grouped_data)


class CreateKeyDoFn(beam.DoFn):
    """
    Creates key-value pairs from input elements by constructing the key as a tuple of values 
    from specified columns, suitable for operations requiring tuple-based keys.
    """
    def __init__(self, key_columns):
        """
        Initializes the CreateKeyDoFn instance.
        
        Args:
            key_columns (list of str): Column names to be used for creating the key.
        """
        self.key_columns = key_columns

    def process(self, element):
        """
        Processes each element to create a key that is a tuple of values from the specified 
        key columns, then yields the key and the element as a tuple.
        
        Args:
            element (tuple): The input element to process, expected to be a tuple where the 
            first item is a dictionary.
        """
        dictionary = element[0]
        key = tuple(dictionary[k] for k in self.key_columns)
        yield (key, element)


class InnerJoinFn(beam.DoFn):
    """
    Implements an inner join between two datasets (TABELA1 and TABELA2) grouped by a common key.
    """
    def __init__(self, columns_to_include=None):
        """
        Initializes the InnerJoinFn instance.
        
        Args:
            columns_to_include (list of str, optional): Columns from TABELA2 to include in the 
            output. Defaults to None, meaning all columns are included.
        """
        self.columns_to_include = columns_to_include

    def process(self, element):
        """
        For each key-grouped element, performs an inner join, combining records from TABELA1 
        and TABELA2 that share a common key.
        
        Args:
            element (tuple): The key and the grouped values from both TABELA1 and TABELA2.
        """
        key, grouped_values = element
        tabela1_values = grouped_values['TABELA1']
        tabela2_values = grouped_values['TABELA2']

        if tabela1_values and tabela2_values:
            for tabela1 in tabela1_values:
                tabela1_value = tabela1[1]  # Unpack the tuple, assuming the record is the second element

                for tabela2 in tabela2_values:
                    tabela2_value = tabela2[1]  # Assuming the record is the second element

                    if self.columns_to_include is not None:
                        filtered_tabela2_value = {k: v for k, v in tabela2_value.items() if k in self.columns_to_include}
                    else:
                        filtered_tabela2_value = tabela2_value

                    yield {**tabela1_value, **filtered_tabela2_value}


class LeftJoinFn(beam.DoFn):
    """
    Implements a left join operation between two datasets, ensuring all records from TABELA1 
    appear in the output, with matched records from TABELA2 where available.
    """
    def __init__(self, columns_to_include=None):
        """
        Initializes the LeftJoinFn instance.
        
        Args:
            columns_to_include (list of str, optional): Columns from TABELA2 to include in the 
            output. Defaults to None, meaning all columns are included.
        """
        self.columns_to_include = columns_to_include

    def process(self, element):
        """
        For each key-grouped element, performs a left join. Outputs each TABELA1 record with 
        matched TABELA2 records merged in based on the common key.
        
        Args:
            element (tuple): The key and the grouped values from both TABELA1 and TABELA2.
        """
        key, grouped_values = element
        tabela1_values = grouped_values['TABELA1']
        tabela2_values = grouped_values['TABELA2']

        for tabela1 in tabela1_values:
            tabela1_value = tabela1[1]  # Unpack the tuple, assuming the record is the second element

            if tabela2_values:
                for tabela2 in tabela2_values:
                    tabela2_value = tabela2[1]

                    # Filter the columns of tabela2_value if columns_to_include is provided
                    if self.columns_to_include is not None:
                        filtered_tabela2_value = {k: v for k, v in tabela2_value.items() if k in self.columns_to_include}
                    else:
                        filtered_tabela2_value = tabela2_value

                    yield {**tabela1_value, **filtered_tabela2_value}
            else:
                yield tabela1_value


class SplitColumnFn(beam.DoFn):
    """
    A DoFn for splitting a column's value into multiple separate parts based on a specified delimiter
    and assigning each part to new columns in the input element. This transformation is useful when a
    single column contains composite data that needs to be separated for further analysis or processing.
    
    The class is flexible to handle any number of output columns up to five and will not break if fewer
    new column names are provided. It also allows specifying a custom delimiter, with '/' as the default.
    
    Attributes:
        column_to_split (str): The name of the column whose value is to be split.
        new_columns (list of str): A list containing the names of the new columns to store the split parts.
        delimiter (str): The delimiter to use for splitting the column's value (default: '/').
    """
    
    def __init__(self, column_to_split, new_columns, delimiter='/'):
        """
        Initializes the SplitColumnFn with the column to split, the names of the new columns, and the delimiter.
        
        Args:
            column_to_split (str): The name of the column to split.
            new_columns (list of str): The names of the new columns for the split parts.
            delimiter (str, optional): The delimiter to use for splitting the value (default: '/').
        """
        self.column_to_split = column_to_split
        self.new_columns = new_columns
        self.delimiter = delimiter

    def process(self, element):
        """
        Splits the specified column's value using the provided delimiter and assigns the resulting parts
        to new columns in the element. If there are fewer parts than new columns, the remaining new columns
        are set to None. If there are fewer new column names provided than the parts, only the provided
        new columns are populated.
        
        Args:
            element (dict): An element of the PCollection, expected to be a dictionary where the column
                            to split exists.
                            
        Yields:
            The modified element with new columns added for the split parts of the original column's value.
        """
        # Split the specified column's value using the provided delimiter.
        parts = element.get(self.column_to_split, '').split(self.delimiter)

        # Assigning split parts to new columns, defaulting to None if the part is not available.
        for i, new_column in enumerate(self.new_columns):
            element[new_column] = parts[i] if i < len(parts) else None

        yield element


class MergeColumnsFn(beam.DoFn):
    def __init__(self, merge_instructions):
        """
        Initializes the MergeColumnsFn with a set of merge instructions.
        Each entry in merge_instructions is a tuple containing:
        - The list of column names to be merged.
        - The name of the new column after merging.
        - An optional delimiter to use in the merge, defaulting to an empty string.
        
        :param merge_instructions: List of tuples (list of columns to merge, new column name, delimiter)
        """
        self.merge_instructions = merge_instructions

    def process(self, element):
        """
        Processes each element to merge columns based on the initialized merge instructions.
        """
        for columns_to_merge, new_column_name, delimiter in self.merge_instructions:
            # Join the specified columns with the provided delimiter
            merged_value = delimiter.join([element[col] for col in columns_to_merge])
            element[new_column_name] = merged_value
        yield element


class GenericDeriveCondition(beam.DoFn):
    def __init__(self, column, map, new_column, default_desc='UNKNOWN'):
        """
        Initializes the DoFn with the necessary parameters.

        Parameters:
        - category_column: The name of the column to check for categories.
        - category_map: A dictionary mapping category values to descriptions.
        - default_desc: The default description if the category column does not exist or the value is not in the category_map.
        """
        self.column = column
        self.map = map
        self.new_column = new_column
        self.default_desc = default_desc

    def process(self, element):
        # Check if the specified column exists in the element
        if self.column in element:
            # Get the value from the element
            value = element[self.column]
            # Map the value to a condition, using the default description if the value is not found
            element[self.new_column] = self.map.get(value, self.default_desc)
        else:
            # If the column does not exist, set the description to the default
            element[self.new_column] = self.default_desc

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


class ColumnsToFloatConverter(beam.DoFn):
    def __init__(self, columns_to_float):
        """
        Initializes the ColumnsToStringConverter instance.
        
        Args:
            columns_to_string (list of str): A list of column names whose values should be converted to strings.
        """
        self.columns_to_float = columns_to_float

    def process(self, element):
        """
        Processes each element, converting specified column values to float.
        
        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column in self.columns_to_float:
            if column in element and isinstance(element[column], str):
                try:
                  element[column] = round(float(element[column]), 2)
                except ValueError:
                  pass
        yield element
