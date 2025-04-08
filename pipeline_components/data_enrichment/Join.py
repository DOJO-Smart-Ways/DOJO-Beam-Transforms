import apache_beam as beam

class LeftJoinFn(beam.PTransform):
    """
    Implements a left join between two datasets (TABLE1 and TABLE2) grouped by a common key.
    ...existing documentation...
    """
    def process(self, element):
        ...existing code...