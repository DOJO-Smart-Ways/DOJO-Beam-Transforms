import apache_beam as beam

class HandleNaNValues(beam.DoFn):
    """
    A DoFn that handles and cleans NaN values in a PCollection element.
    
    This function provides flexibility to handle NaN values in different ways, such as replacing them
    with a default value or removing rows.
    
    Attributes:
        strategy (str): The strategy to handle NaN values. Options are 'replace' or 'remove'.
        default_value (optional): The default value to replace NaN with, if the strategy is 'replace'.
        columns (list of str, optional): The list of columns to apply the strategy. If None, applies to all columns.
    """
    def __init__(self, strategy='replace', default_value=None, columns=None):
        self.strategy = strategy
        self.default_value = default_value
        self.columns = columns

    def process(self, element):
        import math
        import decimal

        columns_to_process = self.columns if self.columns else element.keys()

        if self.strategy == 'replace':
            for key in columns_to_process:
                value = element.get(key)
                if isinstance(value, list):
                    # Replace NaN in lists
                    element[key] = [self.default_value if isinstance(item, float) and math.isnan(item) else item for item in value]
                elif isinstance(value, str) and value.lower() == 'nan':
                    # Replace 'NaN' strings with the default value
                    element[key] = self.default_value
                elif isinstance(value, (float, int, decimal.Decimal)) and math.isnan(value):
                    # Replace numeric NaN values with the default value
                    element[key] = self.default_value

        elif self.strategy == 'remove':
            # Remove rows with any NaN values in the specified columns
            if any(isinstance(element.get(key), (float, int, decimal.Decimal)) and math.isnan(element.get(key)) for key in columns_to_process):
                return
            if any(isinstance(element.get(key), str) and element.get(key).lower() == 'nan' for key in columns_to_process):
                return
            yield element

        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")

        yield element
