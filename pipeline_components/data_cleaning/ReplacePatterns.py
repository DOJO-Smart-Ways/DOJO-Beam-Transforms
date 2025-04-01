import apache_beam as beam

class ReplacePatterns(beam.DoFn):
    """
    A class that extends DoFn to replace specific patterns in the values of specified columns 
    for each element in the PCollection.

    Parameters:
    - columns: A list of column names where the patterns will be replaced.
    - patterns: A dictionary where keys are patterns to search for and values are the replacements.
    """
    def __init__(self, columns, patterns):
        self.columns = columns
        self.patterns = patterns

    def process(self, element):
        # Replace patterns in the specified columns
        for column in self.columns:
            if column in element:
                for pattern, replacement in self.patterns.items():
                    element[column] = element[column].replace(pattern, replacement)
        yield element
