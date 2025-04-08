import apache_beam as beam
from decimal import Decimal, InvalidOperation


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
    Implements an inner join between two datasets (TABLE1 and TABLE2) grouped by a common key.
    """
    def __init__(self, columns_to_include=None):
        """
        Initializes the InnerJoinFn instance.
        
        Args:
            columns_to_include (list of str, optional): Columns from TABLE2 to include in the 
            output. Defaults to None, meaning all columns are included.
        """
        self.columns_to_include = columns_to_include

    def process(self, element):
        """
        For each key-grouped element, performs an inner join, combining records from TABLE1 
        and TABLE2 that share a common key.
        
        Args:
            element (tuple): The key and the grouped values from both TABLE1 and TABLE2.
        """
        key, grouped_values = element
        table1_values = grouped_values['TABLE1']
        table2_values = grouped_values['TABLE2']

        if table1_values and table2_values:
            for table1 in table1_values:
                table1_value = table1[1]  # Unpack the tuple, assuming the record is the second element

                for table2 in table2_values:
                    table2_value = table2[1]  # Assuming the record is the second element

                    if self.columns_to_include is not None:
                        filtered_table2_value = {k: v for k, v in table2_value.items() if k in self.columns_to_include}
                    else:
                        filtered_table2_value = table2_value

                    yield {**table1_value, **filtered_table2_value}


class LeftJoinFn(beam.DoFn):
    """
    Implements a left join operation between two datasets, ensuring all records from TABLE1 
    appear in the output, with matched records from TABLE2 where available.
    """
    def __init__(self, columns_to_include=None):
        """
        Initializes the LeftJoinFn instance.
        
        Args:
            columns_to_include (list of str, optional): Columns from TABLE2 to include in the 
            output. Defaults to None, meaning all columns are included.
        """
        self.columns_to_include = columns_to_include

    def process(self, element):
        """
        For each key-grouped element, performs a left join. Outputs each TABLE1 record with 
        matched TABLE2 records merged in based on the common key.
        
        Args:
            element (tuple): The key and the grouped values from both TABLE1 and TABLE2.
        """
        key, grouped_values = element
        table1_values = grouped_values['TABLE1']
        table2_values = grouped_values['TABLE2']

        # Determine the full set of columns to include from TABLE2
        all_columns = self.columns_to_include if self.columns_to_include is not None else None

        for table1 in table1_values:
            table1_value = table1[1]  # Assuming the record is the second element in the tuple

            if table2_values:
                for table2 in table2_values:
                    table2_value = table2[1]
                    if all_columns is not None:
                        # Include only specified columns from TABLE2
                        filtered_table2_value = {k: v for k, v in table2_value.items() if k in all_columns}
                    else:
                        filtered_table2_value = table2_value
                        
                    for k in filtered_table2_value.keys():
                        if filtered_table2_value[k] is None:
                            filtered_table2_value[k] = ''
                    yield {**table1_value, **filtered_table2_value}

            else:
                # Ensure the output format is consistent, even for TABLE1 records with no TABLE2 match
                if all_columns is not None:
                    no_match_table2_value = {k: '' for k in all_columns}
                    yield {**table1_value, **no_match_table2_value}
                else:
                    yield table1_value


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
    
    def __init__(self, column_to_split, new_columns, delimiter=',', limit_splits=None):
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
        self.limit_splits = limit_splits

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
        
        if self.limit_splits is None:
          parts = element.get(self.column_to_split, '').split(self.delimiter)
        else:
          parts = element.get(self.column_to_split, '').split(self.delimiter, self.limit_splits)

        # Assigning split parts to new columns, defaulting to None if the part is not available.
        for i, new_column in enumerate(self.new_columns):
            element[new_column] = parts[i] if i < len(parts) else None

        yield element

def join(table1, table2, method='left_join', columns_to_include=None):
    """Performs a join between two PCollections based on a common key and the specified method.

    Args:
        table1: The first PCollection to join.
        table2: The second PCollection to join.
        method (str): The join method, either 'left_join' or 'inner_join'.
        columns_to_include (list): To include specific columns in join.

    Returns:
        A PCollection resulting from the specified join of table1 and table2.
    """
    if method == 'left_join':
        join_fn = LeftJoinFn(columns_to_include)
    elif method == 'inner_join':
        join_fn = InnerJoinFn(columns_to_include)
    else:
        raise ValueError("Unsupported join method: {}. Use 'left_join' or 'inner_join'.".format(method))

    def apply_join(p_collection):
        result = (p_collection
                  | f"CoGroupByKey {table1} {table2} {method}" >> beam.CoGroupByKey()
                  | f"Apply Join Logic {table1} {table2} {method}" >> beam.ParDo(join_fn))
        return result

    return apply_join({'TABLE1': table1, 'TABLE2': table2})


def key_transform(p_collection, key_columns, identifier=''):
    """Applies a composite key creation and a subsequent key transformation on a PCollection.

    Args:
        p_collection: The input PCollection to transform.
        key_columns (list of str): The columns to use for creating the composite key.

    Returns:
        A PCollection with elements keyed by the specified columns.
    """
    identifier_suffix = f"_{identifier}" if identifier else ""
    return (p_collection
            | f"Create Composite Key {key_columns} on {identifier_suffix}" >> beam.ParDo(KeyByComposite(key_columns))
            | f"Transform Key {key_columns} on {identifier_suffix}" >> beam.ParDo(CreateKeyDoFn(key_columns)))

