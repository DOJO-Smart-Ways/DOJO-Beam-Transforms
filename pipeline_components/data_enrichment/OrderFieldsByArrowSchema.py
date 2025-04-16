import apache_beam as beam

class OrderFieldsByArrowSchema(beam.DoFn):
    """
    A DoFn that orders the fields of an element according to an Apache Arrow schema.

    This class ensures that the fields in the output dictionary are ordered
    based on the field names defined in the provided Arrow schema.

    Args:
        arrow_schema: An Apache Arrow schema object containing the field names.
    """
    def __init__(self, arrow_schema):
        """
        Initializes the OrderFieldsByArrowSchema with an Arrow schema.

        Args:
            arrow_schema: An Apache Arrow schema object containing the field names.
        """
        self.schema_fields = [field.name for field in arrow_schema]
 
    def process(self, element):
        """
        Orders the fields of the element according to the Arrow schema.

        Args:
            element: A dictionary representing a single record.

        Yields:
            A dictionary with keys ordered according to the Arrow schema.
        """
        ordered_dict = {field: element[field] if field in element else None for field in self.schema_fields}
        yield ordered_dict
