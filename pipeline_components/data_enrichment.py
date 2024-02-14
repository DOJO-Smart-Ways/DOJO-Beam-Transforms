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
