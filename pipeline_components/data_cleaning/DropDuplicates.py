import apache_beam as beam

class DropDuplicates(beam.DoFn):
    """
    A DoFn that removes duplicate elements from a PCollection based on specified columns or the entire element.
    
    This function is useful for data cleaning and preprocessing, where you may want to ensure that each element
    in the PCollection is unique based on certain criteria.
    
    Attributes:
        columns (list of str, optional): A list of column names to consider for deduplication. If not provided,
                                         the entire element will be used for deduplication.
    """
    
    def __init__(self, columns=None):
        """
        Initializes the DropDuplicates instance with the columns to consider for deduplication.
        
        Args:
            columns (list of str, optional): The names of the columns to consider for deduplication.
                                             If not provided, the entire element will be used.
        """
        self.seen = set()
        self.columns = columns

    def process(self, element):
        """
        Processes each element in the PCollection, yielding only unique elements based on the specified columns
        or the entire element.
        
        Args:
            element (dict): An element of the PCollection, expected to be a dictionary.
            
        Yields:
            The unique element if it has not been seen before; otherwise, nothing is yielded.
        """
        # If columns are specified, create a sub-dictionary with only those columns
        if self.columns and isinstance(element, dict):
            sub_element = {col: element[col] for col in self.columns if col in element}
            hashable_element = tuple(sorted(sub_element.items()))
        else:
            # Convert the element to a hashable type if it's a dictionary
            if isinstance(element, dict):
                hashable_element = tuple(sorted(element.items()))
            else:
                hashable_element = element

        if hashable_element not in self.seen:
            self.seen.add(hashable_element)
            yield element
