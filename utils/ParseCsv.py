import apache_beam as beam
import csv

class ParseCsv(beam.DoFn):
    def __init__(self, column_names, delimiter=';'):
        self.column_names = column_names
        self.delimiter = delimiter

    def process(self, element):
        for row in csv.reader([element], delimiter=self.delimiter):
            if row:
                yield {name: row[idx] for idx, name in enumerate(self.column_names)}