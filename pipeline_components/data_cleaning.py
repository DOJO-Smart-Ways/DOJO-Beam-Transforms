import apache_beam as beam
import re
from decimal import Decimal, InvalidOperation


class ChangeDateFormat(beam.DoFn):
    def __init__(self, date_columns, input_format, output_format='%Y-%m-%d'):

        """
        Initialize the DoFn class.

        Parameters:
        - date_column: The name of the column containing the date string.
        - input_format: The strftime format string for the input date format.
        - output_format: The strftime format string for the output date format. Defaults to '%Y-%m-%d'.
        """
        self.date_columns = date_columns
        self.input_format = input_format
        self.output_format = output_format

    def process(self, element):
        from datetime import datetime
        """
        Process each element to format the dates in the specified columns.

        Parameters:
        - element: The input element to process.
        """
        for date_column in self.date_columns:
            date_str = element.get(date_column)
            if date_str:
                try:
                    # Parse the date using the input format
                    date_obj = datetime.strptime(date_str, self.input_format)
                    # Format the date into the desired output format
                    formatted_date = date_obj.strftime(self.output_format)
                    # Update the element with the formatted date
                    element[date_column] = formatted_date
                except ValueError:
                    # Handle the case where the date format is incorrect
                    # In a real scenario, you might want to log this or handle it differently
                    pass
        yield element

class HandleNaN(beam.DoFn):
    """
    Treatment of NaN values ​​in the column
    """
    def process(self, element):
        import math
        for key, value in element.items():
            if isinstance(value, list):
                # Replace nan in lists
                element[key] = ["" if isinstance(item, float) and math.isnan(item) else item for item in value]
            elif value is None or (isinstance(value, float) and math.isnan(value)):
                # Replace single nan values
                element[key] = ''

        yield element

class FilterColumnValues(beam.DoFn):
    """
    A DoFn that filters elements based on specified values in a given column.
    
    This function allows for selective filtering of records in a PCollection where
    only elements with a specific column containing a value from a predefined list 
    are retained. This is particularly useful for data cleaning and preprocessing,
    where you may want to exclude records based on certain criteria.
    
    Attributes:
        column (str): The name of the column to check for specified values.
        values_to_filter (list of str): A list of string values. If the value in the
                                        specified column matches any value in this list,
                                        the element will be filtered out.
    """
    
    def __init__(self, column, values_to_filter):
        """
        Initializes the FilterColumnValues instance with the column name and values to filter.
        
        Args:
            column (str): The name of the column to check against the values_to_filter.
            values_to_filter (list of str): The list of values to filter by; elements with these
                                            values in the specified column will be excluded.
        """
        self.column = column
        self.values_to_filter = values_to_filter

    def process(self, element):
        """
        Processes each element in the PCollection, yielding those that do not match the filter criteria.
        
        Args:
            element (dict): An element of the PCollection, expected to be a dictionary with keys
                            corresponding to column names.
                            
        Yields:
            The element itself if its value in the specified column does not match any of the
            values in the values_to_filter list; otherwise, nothing is yielded for that element.
        """
        # Check if the element's value for the specified column is not in the list of values to filter
        if element.get(self.column) not in self.values_to_filter:
            yield element
            

class DropColumns(beam.DoFn):
    """
    A DoFn for dropping specified columns from each element in a PCollection.
    
    This function is useful in scenarios where certain columns of data are not needed
    for further processing or analysis, allowing for the reduction of data volume and
    simplification of the data structure.
    
    Attributes:
        column (list of str): A list containing the names of the columns to be dropped.
    """
    
    def __init__(self, column):
        """
        Initializes the DropColumns instance with the names of columns to drop.
        
        Args:
            column (list of str): The names of the columns to be dropped from each element.
                                  Can be a single string if only one column needs to be dropped.
        """
        # Ensure column is a list to simplify processing
        self.column = column if isinstance(column, list) else [column]

    def process(self, element):
        """
        Processes each element in the PCollection, dropping specified columns.
        
        Args:
            element (dict): An element of the PCollection, expected to be a dictionary
                            from which specified columns will be removed.
                            
        Yields:
            The modified element with specified columns removed.
        """
        # Iterate over the list of columns to drop and remove them from the element
        for col in self.column:
            element.pop(col, None)  # Use pop with None as default to avoid KeyError if the column is missing
        yield element


class KeepColumns(beam.DoFn):
    """
    A DoFn for keeping specified columns from each element in a PCollection.

    This function is useful in scenarios where certain columns of data are needed
    for further processing or analysis, allowing for the reduction of data volume and
    simplification of the data structure.

    Attributes:
        columns (list of str): A list containing the names of the columns to be kept.
    """

    def __init__(self, columns):
        """
        Initializes the KeepColumns instance with the names of columns to keep.

        Args:
            columns (list of str): The names of the columns to be kept from each element.
                                   Can be a single string if only one column needs to be kept.
        """
        # Ensure columns is a list to simplify processing
        self.columns = columns if isinstance(columns, list) else [columns]

    def process(self, element):
        """
        Processes each element in the PCollection, keeping only specified columns.

        Args:
            element (dict): An element of the PCollection, expected to be a dictionary
                            with keys corresponding to column names.

        Yields:
            A dictionary containing only the specified columns to be kept.
        """
        # Create a new dictionary with only the columns to keep
        result = {col: element[col] for col in self.columns if col in element}
        yield result


class CleanNaN(beam.DoFn):
    def process(self, element, *args, **kwargs):
        import math
        import decimal
        # Elemento é um dicionário representando uma linha, com coluna: valor
        for key, value in element.items():
            if isinstance(value, str) and value.lower() == 'nan':
                # Caso 1: Para strings que contêm 'NaN', substitui por uma string vazia
                element[key] = ''
            elif isinstance(value, (float, int, decimal.Decimal)) and math.isnan(value):
                # Caso 2: Para números (incluindo decimal, float, integer), substitui por None
                element[key] = None
        yield element

#TODO: Remove DeriveSingleValue method from data cleaning. New method was moved to Data Enrichment with new name ColumnValueAssignment
class DeriveSingleValue(beam.DoFn):
    def __init__(self, value, new_column):
        """
        Inicializa o DoFn com os parâmetros necessários.

        Parâmetros:
        - value: O valor único a ser atribuído à nova coluna.
        - new_column: O nome da nova coluna.
        """
        self.value = value
        self.new_column = new_column

    def process(self, element):
        # Atribui o valor único à nova coluna
        element[self.new_column] = self.value
        yield element
#################################################################################################################
# RenameColumns is designed to rename columns in a PCollection's element (expected to be a dictionary). 
# The renaming is defined by a column_mapping dictionary where keys are original column names, and 
# values are the new names for these columns.
#################################################################################################################
class RenameColumns(beam.DoFn):
    def __init__(self, column_mapping):
        """
        Initializes the RenameColumns instance.
        
        Args:
            column_mapping (dict): A dictionary mapping from old column names to new column names.
        """
        self.column_mapping = column_mapping

    def process(self, element):
        """
        Processes each element, renaming columns as specified in column_mapping.
        
        Args:
            element (dict): The input element to process, where keys are column names.
        """
        new_element = {self.column_mapping.get(k, k): v for k, v in element.items()}
        yield new_element

#################################################################################################################
# ReplaceValues is used to replace specific values within specified columns of a PCollection's element 
# (expected to be a dictionary). The replacements are defined in a list of tuples, where each tuple contains 
# the column name, the value to replace, and the new value.
#################################################################################################################
class ReplaceValues(beam.DoFn):
    def __init__(self, replacements):
        """
        Initializes the ReplaceValues instance.
        
        Args:
            replacements (list of tuples): A list where each tuple contains (column name, current value, replacement value).
        """
        self.replacements = replacements

    def process(self, element):
        """
        Processes each element, replacing specified values within specified columns.
        
        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column, current_value, replacement in self.replacements:
            if column in element:
                column_value = element[column]
                if isinstance(column_value, str) and current_value in column_value:
                    element[column] = column_value.replace(current_value, replacement)
        yield element

class KeepColumnValues(beam.DoFn):
    """
    A DoFn that filters elements based on specified values in a given column.

    This function allows for selective filtering of records in a PCollection where
    only elements with a specific column containing a value from a predefined list
    are retained. This is particularly useful for data cleaning and preprocessing,
    where you may want to exclude records based on certain criteria.

    Attributes:
        column (str): The name of the column to check for specified values.
        values_to_filter (list of str): A list of string values. If the value in the
                                        specified column matches any value in this list,
                                        the element will be filtered out.
    """

    def __init__(self, column, values_to_filter):
        """
        Initializes the FilterColumnValues instance with the column name and values to filter.

        Args:
            column (str): The name of the column to check against the values_to_filter.
            values_to_filter (list of str): The list of values to filter by; elements with these
                                            values in the specified column will be excluded.
        """
        self.column = column
        self.values_to_filter = values_to_filter

    def process(self, element):
        """
        Processes each element in the PCollection, yielding those that do not match the filter criteria.

        Args:
            element (dict): An element of the PCollection, expected to be a dictionary with keys
                            corresponding to column names.

        Yields:
            The element itself if its value in the specified column does not match any of the
            values in the values_to_filter list; otherwise, nothing is yielded for that element.
        """
        # Check if the element's value for the specified column is not in the list of values to filter
        if element.get(self.column) in self.values_to_filter:
            yield element


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


class DeduplicateFn(beam.DoFn):
    # this only works if all values in the dictionary are themselves hashable.
    def __init__(self):
        self.seen = set()

    def process(self, element):
        # Convert the element to a hashable type if it's a dictionary
        if isinstance(element, dict):
            hashable_element = tuple(sorted(element.items()))
        else:
            hashable_element = element

        if hashable_element not in self.seen:
            self.seen.add(hashable_element)
            yield element


class ExtractDecimalFromString(beam.DoFn):
    """
    A custom Apache Beam transformation to extract decimal values from one or more fields in an element.

    Args:
        columns: A list of column names to be processed.
    """

    def process(self, element, columns):
        """
        Processes an element, extracts decimal values from the specified fields, and replaces them with the extracted values.

        Args:
            element: The element to be processed.
            columns: A list Variable arguments containing the names of the columns to be processed.

        Yields:
            A new element with the extracted decimal values.
        """

        for column in columns:
            if column in element:
                input_string = element[column]
                cleaned_value = self.extract_decimal(input_string)
                if cleaned_value is not None:
                    element[column] = cleaned_value
                else:
                    element[column] = Decimal(0.0)  # Assign 0 when cleaned_value is None or empty string
        yield element

    def extract_decimal(self, input_string):
        """
        Extracts a decimal value from an input string.

        Args:
            input_string: The input string to be processed.

        Returns:
            A Decimal object representing the extracted value, or None if extraction fails.
        """
        if input_string is None or input_string == '':
            return Decimal(0.0)  # Return 0 if input_string is None or empty string
        
        # Handle negative values enclosed in parentheses using regex
        cleaned_string = re.sub(r'\((.*?)\)', lambda x: '-' + x.group(1), input_string)
        # Replace all non-numeric characters except commas and minus signs with an empty string
        cleaned_string = re.sub(r'[^\d,-]', '', cleaned_string).replace(',', '.')
        
        try:
            # Convert the cleaned string to a Decimal without rounding
            return Decimal(cleaned_string)
        except InvalidOperation:
            # If conversion to Decimal fails, raise an error message and return None
            raise ValueError(f"Invalid value: {input_string} (cleaned: {cleaned_string})")

    """
    Usage Examples:
    - Extracting decimal values from a dataset:
        pipeline = beam.Pipeline()
        data = [{'amount': '$1,234.56'}, {'amount': '(1,500.25)'}]
        extracted_data = (
            pipeline
            | 'Create Data' >> beam.Create(data)
            | 'Extract Decimals' >> beam.ParDo(ExtractDecimalFn(), 'amount')
            | beam.Map(print)
        )
        pipeline.run()

    - Extracting and cleaning monetary values:
        pipeline = beam.Pipeline()
        data = [{'amount': '$1.234,56'}, {'amount': '(1.500,25)'}]
        cleaned_data = (
            pipeline
            | 'Create Data' >> beam.Create(data)
            | 'Extract Decimals' >> beam.ParDo(ExtractDecimalFn(), 'amount')
            | beam.Filter(lambda x: x['amount'] is not None)  # Filtering out invalid values
            | beam.Map(lambda x: {'cleaned_amount': x['amount']})  # Optionally, store cleaned values in a new field
            | beam.Map(print)
        )
        pipeline.run()

    Notes:
    - This method is particularly useful for handling monetary values, as it avoids rounding discrepancies.
    - It extracts decimal values from input strings, including cases where negative values are represented within parentheses.
    - If decimal values are already separeted by dots, please try a simple cast to Decimal Type. 
    """


class TrimValues(beam.DoFn):
    def __init__(self, columns):
        """
        Initializes the TrimValues instance.

        Args:
            columns (list of str): A list of column names to trim spaces from.
        """
        self.columns = columns

    def process(self, element):
        """
        Processes each element, trimming leading and trailing spaces from specified columns.

        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column in self.columns:
            if column in element and isinstance(element[column], str):
                element[column] = element[column].strip()
        yield element


class RemoveAccents(beam.DoFn):
    """
    A class to remove accents from specified columns of elements.

    Attributes:
        columns (list): A list of column names from which accents will be removed.
    """

    def __init__(self, columns):
        """
        Initializes the RemoveAccents object with the specified columns.

        Args:
            columns (list): A list of column names from which accents will be removed.
        """
        self.columns = columns

    def process(self, element):
        """
        Processes each element in the pipeline, removing accents from specified columns.

        Args:
            element (dict): The element to be processed.

        Returns:
            list: A list containing the processed element with accents removed.
        """
        from unidecode import unidecode
        try:
            for col in self.columns:
                if col in element and element[col] is not None:
                    element[col] = unidecode(str(element[col]))
        except Exception as e:
            print(f"Error while removing accents: {e}")
        return [element]

