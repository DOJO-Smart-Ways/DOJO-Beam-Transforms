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

        if self.strategy == 'remove':
            # Remove rows with any NaN values in the specified columns
            for key in columns_to_process:
                value = element.get(key)
                if (
                    (isinstance(value, (float, int, decimal.Decimal)) and math.isnan(value)) or
                    (isinstance(value, str) and value.lower() == 'nan')
                ):
                    # Skip this element if any column contains NaN
                    return
            # Yield the element if no NaN values are found
            yield element

        elif self.strategy == 'replace':
            # Replace NaN values with the default value
            for key in columns_to_process:
                value = element.get(key)
                if (
                    (isinstance(value, (float, int, decimal.Decimal)) and math.isnan(value)) or
                    (isinstance(value, str) and value.lower() == 'nan')
                ):
                    element[key] = self.default_value
            yield element

        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")
