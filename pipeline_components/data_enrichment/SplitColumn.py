import apache_beam as beam

class SplitColumn(beam.DoFn):
    """
    A DoFn that splits a column's value into multiple parts based on a delimiter
    and assigns the resulting values either as a list to a single column or to multiple columns.
    """
    def __init__(self, column_to_split, new_columns, delimiter, as_list=True):
        """
        Initializes the SplitColumn DoFn.

        Args:
            column_to_split (str): The name of the column to split.
            new_columns (list): The name(s) of the new column(s) to assign the split values.
            delimiter (str): The delimiter to use for splitting the column's value.
            as_list (bool): If True, assign the split values as a list to the first new column.
                            If False, assign the split values to multiple columns.
        """
        self.column_to_split = column_to_split
        self.new_columns = new_columns
        self.delimiter = delimiter
        self.as_list = as_list

    def process(self, element):
        """
        Processes each element, splitting the specified column's value and assigning
        the resulting values to the new column(s).

        Args:
            element (dict): The input element.

        Yields:
            dict: The modified element with the new column(s) added.
        """
        if self.column_to_split in element:
            split_values = element[self.column_to_split].split(self.delimiter)
            if self.as_list:
                # Assign the list of split values to the first new column
                element[self.new_columns[0]] = split_values
            else:
                # Assign the split values to multiple columns
                for i, column in enumerate(self.new_columns):
                    element[column] = split_values[i] if i < len(split_values) else None
        yield element