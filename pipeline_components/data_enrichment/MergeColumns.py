import apache_beam as beam

class MergeColumns(beam.DoFn):
    """
    A Beam DoFn class for merging multiple columns into a single column with a specified delimiter.
    """
    
    def __init__(self, params):
        """
        Initializes the class with parameters for merging columns.
        
        Args:
            params (list of tuples): Each tuple contains:
                - columns (list of str): Columns to merge.
                - result_column (str): The name of the resulting column.
                - delimiter (str): The delimiter to use for merging.
        """
        self.params = params

    def process(self, element):
        """
        Processes each element to merge specified columns.
        """
        for columns, result_column, delimiter in self.params:
            # Check if the result column already exists
            if result_column in element:
                raise ValueError(f"Target column '{result_column}' already exists in the element: {element}")
            
            # Check if all source columns exist
            for col in columns:
                if col not in element:
                    raise KeyError(f"Missing column '{col}' in the element: {element}")
            
            # Merge columns with the specified delimiter
            try:
                element[result_column] = delimiter.join(str(element[col]) for col in columns)
            except Exception as e:
                raise ValueError(f"Error merging columns {columns} into '{result_column}': {e}")
        
        yield element