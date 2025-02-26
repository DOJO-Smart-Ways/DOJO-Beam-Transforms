import apache_beam as beam

class ReplaceStartWithFn(beam.DoFn):
    def __init__(self, start_with, replace_with, columns):
        self.start_with = start_with        
        self.replace_with = replace_with
        self.columns = columns

    def process(self, element):
        for column in self.columns:
            # Check if the column exists in the element to avoid KeyError
            if column in element and isinstance(element[column], str):
                # Check if the value starts with the specified prefix
                if element[column].startswith(self.start_with):
                    # If so, replace the prefix with 'replace_with'. This specifically handles removing the prefix correctly.
                    element[column] = self.replace_with + element[column][len(self.start_with):]
        yield element
