import apache_beam as beam

class MergeColumns(beam.DoFn):
    """
    A DoFn that merges the values of specified columns into a new column using a delimiter.

    Attributes:
        merge_instructions (list): A list of tuples, where each tuple contains:
            - A list of column names to merge.
            - The name of the new column to store the merged value.
            - An optional delimiter to use for merging (default: an empty string).
    """
    def __init__(self, merge_instructions):
        """
        Initializes the MergeColumns instance with a set of merge instructions.

        Args:
            merge_instructions (list): A list of tuples (columns_to_merge, new_column_name, delimiter).
        """
        self.merge_instructions = merge_instructions

    def process(self, element):
        """
        Processes each element to merge specified columns based on the merge instructions.

        Args:
            element (dict): The input element to process.

        Yields:
            dict: The modified element with the new merged columns added.
        """
        try:
            for columns_to_merge, new_column_name, delimiter in self.merge_instructions:
                # Check if all columns to merge exist in the element
                if all(col in element for col in columns_to_merge):
                    try:
                        # Join the specified columns with the provided delimiter
                        merged_value = delimiter.join([str(element[col]) for col in columns_to_merge])
                        element[new_column_name] = merged_value
                    except Exception as e:
                        raise ValueError(f"Error merging columns {columns_to_merge} into '{new_column_name}': {e}")
                else:
                    # Raise an error if any column is missing
                    missing_columns = [col for col in columns_to_merge if col not in element]
                    raise KeyError(f"Missing columns {missing_columns} in element: {element}")
        except Exception as e:
            raise ValueError(f"Error processing element {element}: {e}")

        yield element