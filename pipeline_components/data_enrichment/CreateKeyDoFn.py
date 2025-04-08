import apache_beam as beam

class CreateKeyDoFn(beam.DoFn):
    """
    Creates key-value pairs from input elements by constructing the key as a tuple of values 
    from specified columns, suitable for operations requiring tuple-based keys.
    ...existing documentation...
    """
    def process(self, element):
        ...existing code...