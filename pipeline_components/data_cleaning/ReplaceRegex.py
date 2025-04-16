import apache_beam as beam
import re

class ReplaceRegex(beam.DoFn):
    """
    A class that extends DoFn to replace regex patterns in the values of specified columns 
    for each element in the PCollection.

    Parameters:
    - columns: A list of column names where the regex patterns will be replaced.
    - patterns: A list of tuples, where the first element is a regex pattern, 
                and the second element is the replacement value.
    """
    def __init__(self, columns, patterns):
        # Validate columns
        if not isinstance(columns, list):
            raise TypeError(f"Columns must be a list, but got {type(columns).__name__}.")
        if not all(isinstance(col, str) for col in columns):
            raise ValueError("All columns must be strings.")
        self.columns = columns

        # Validate patterns
        if not isinstance(patterns, list):
            raise TypeError(f"Patterns must be a list, but got {type(patterns).__name__}.")
        if not all(isinstance(pattern, tuple) and len(pattern) == 2 for pattern in patterns):
            raise ValueError("Each pattern must be a tuple with a regex pattern and a replacement value.")
        self.compiled_patterns = self._compile_patterns(patterns)

    def _compile_patterns(self, patterns):
        """
        Pre-compiles regex patterns.

        Parameters:
        - patterns: A list of tuples, where the first element is a regex pattern 
                    and the second element is the replacement value.

        Returns:
        - A list of tuples, where the first element is a compiled regex, 
          and the second element is the replacement value.

        Raises:
        - ValueError: If any pattern is not a valid regex.
        """
        compiled_patterns = []
        for pattern, replacement in patterns:
            try:
                compiled_patterns.append((re.compile(pattern), replacement))
            except re.error as e:
                raise ValueError(f"Invalid regex pattern: {pattern}. Error: {e}")
        return compiled_patterns

    def process(self, element):
        # Replace regex patterns in the specified columns
        for column in self.columns:
            if column in element:
                for regex, replacement in self.compiled_patterns:
                    element[column] = regex.sub(replacement, element[column])
        yield element
