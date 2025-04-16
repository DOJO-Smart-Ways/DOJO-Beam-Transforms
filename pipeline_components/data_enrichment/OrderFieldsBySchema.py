import apache_beam as beam

class OrderFieldsBySchema(beam.DoFn):
    def __init__(self, schema_str):
        """
        Initializes the OrderFieldsBySchema with a schema string.

        Args:
            schema_str (str): A multiline string representing the schema.
        """
        # Parse the schema string to extract field names
        self.schema_fields = [line.split(":")[0].strip() for line in schema_str.strip().split("\n")]

    def process(self, element):
        """
        Orders the fields of the element according to the schema, maintaining a dictionary format.

        Args:
            element: A dictionary representing a single record.

        Yields:
            A dictionary with keys ordered according to the schema.
        """
        ordered_dict = {field: element.get(field, None) for field in self.schema_fields}
        yield ordered_dict