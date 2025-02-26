import apache_beam as beam

class ReplacePatterns(beam.DoFn):
    def __init__(self, columns, pattern, replacement):
        self.columns = columns
        self.pattern = pattern
        self.replacement = replacement

    def process(self, element):
        import re
        for column in self.columns:
            if column in element and re.match(self.pattern, element[column]):
                element[column] = re.sub(self.pattern, self.replacement, element[column])
        yield element
