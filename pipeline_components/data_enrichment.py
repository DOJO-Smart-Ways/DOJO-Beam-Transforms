import apache_beam as beam

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
