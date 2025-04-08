import apache_beam as beam

class SplitColumn(beam.DoFn):
    """
    A DoFn for splitting a column's value into multiple separate parts based on a specified delimiter
    and assigning each part to new columns in the input element. This transformation is useful when a
    single column contains composite data that needs to be separated for further analysis or processing.

    Attributes:
        column_to_split (str): The name of the column whose value is to be split.
        new_columns (list of str): A list containing the names of the new columns to store the split parts.
        delimiter (str): The delimiter to use for splitting the column's value (default: ',').
        limit_splits (int, optional): The maximum number of splits to perform (default: None, meaning no limit).
    """
    def __init__(self, column_to_split, new_columns, delimiter=',', limit_splits=None):
        """
        Initializes the SplitColumn with the column to split, the names of the new columns, and the delimiter.

        Args:
            column_to_split (str): The name of the column to split.
            new_columns (list of str): The names of the new columns for the split parts.
            delimiter (str, optional): The delimiter to use for splitting the value (default: ',').
            limit_splits (int, optional): The maximum number of splits to perform (default: None).
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
            dict: The modified element with new columns added for the split parts of the original column's value.
        """
        try:
            if self.column_to_split not in element:
                raise KeyError(f"Column '{self.column_to_split}' not found in the input element: {element}")

            # Split the column value using the delimiter
            if self.limit_splits is None:
                parts = element.get(self.column_to_split, '').split(self.delimiter)
            else:
                parts = element.get(self.column_to_split, '').split(self.delimiter, self.limit_splits)

            # Assign split parts to new columns, defaulting to None if the part is not available
            for i, new_column in enumerate(self.new_columns):
                element[new_column] = parts[i] if i < len(parts) else None

        except Exception as e:
            raise ValueError(f"Error processing element {element}: {e}")

        yield element