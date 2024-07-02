import apache_beam as beam
from decimal import Decimal, InvalidOperation

class ConvertToUpperCase(beam.DoFn):
    """
    A DoFn that converts all string values in a dictionary element to uppercase.
    
    This function iterates over each key-value pair in the input element. If a value
    is of type string, it converts the string to uppercase. This is useful in data 
    normalization processes where consistent case formatting is required.
    
    The process method is designed to handle any exceptions that occur during the 
    conversion, logging an error message for debugging purposes. This ensures that 
    the pipeline can continue processing other elements even if one element causes 
    an issue.
    """
    
    def process(self, element):
        import logging
        """
        Processes each element in the PCollection, converting all string values to uppercase.
        
        Args:
            element (dict): An element of the PCollection, expected to be a dictionary.
            
        Yields:
            The modified element with all string values converted to uppercase.
        """
        # Create a copy of the keys to iterate over to avoid modifying the dictionary while iterating
        keys = list(element.keys())
        
        for key in keys:
            value = element[key]
            # Check if the value is a string
            try:
                if isinstance(value, str):
                    # Convert the string to uppercase and update the element
                    element[key] = value.upper()
            except Exception as e:
                # Log the error for debugging purposes
                logging.error(f"Error processing key {key} with value {value}: {str(e)}")
        
        yield element

class KeyByComposite(beam.DoFn):
    """
    Transforms input elements into key-value pairs where the key is a composite made from 
    specified columns of the input element, and the value is the entire input element. 
    Useful for grouping or joining data based on multiple columns.
    """
    def __init__(self, key_columns):
        """
        Initializes the KeyByComposite instance.
        
        Args:
            key_columns (list of str): Column names to be used for forming the composite key.
        """
        self.key_columns = key_columns

    def process(self, element):
        """
        For each input element, creates a composite key and yields a tuple of this key 
        and the original element.
        
        Args:
            element (dict): The input element to process.
        """
        composite_key = {k:element[k] for k in self.key_columns if k in element}
        grouped_data = {k:element[k] for k in element}
        yield (composite_key, grouped_data)


class CreateKeyDoFn(beam.DoFn):
    """
    Creates key-value pairs from input elements by constructing the key as a tuple of values 
    from specified columns, suitable for operations requiring tuple-based keys.
    """
    def __init__(self, key_columns):
        """
        Initializes the CreateKeyDoFn instance.
        
        Args:
            key_columns (list of str): Column names to be used for creating the key.
        """
        self.key_columns = key_columns

    def process(self, element):
        """
        Processes each element to create a key that is a tuple of values from the specified 
        key columns, then yields the key and the element as a tuple.
        
        Args:
            element (tuple): The input element to process, expected to be a tuple where the 
            first item is a dictionary.
        """
        dictionary = element[0]
        key = tuple(dictionary[k] for k in self.key_columns)
        yield (key, element)


class InnerJoinFn(beam.DoFn):
    """
    Implements an inner join between two datasets (TABLE1 and TABLE2) grouped by a common key.
    """
    def __init__(self, columns_to_include=None):
        """
        Initializes the InnerJoinFn instance.
        
        Args:
            columns_to_include (list of str, optional): Columns from TABLE2 to include in the 
            output. Defaults to None, meaning all columns are included.
        """
        self.columns_to_include = columns_to_include

    def process(self, element):
        """
        For each key-grouped element, performs an inner join, combining records from TABLE1 
        and TABLE2 that share a common key.
        
        Args:
            element (tuple): The key and the grouped values from both TABLE1 and TABLE2.
        """
        key, grouped_values = element
        table1_values = grouped_values['TABLE1']
        table2_values = grouped_values['TABLE2']

        if table1_values and table2_values:
            for table1 in table1_values:
                table1_value = table1[1]  # Unpack the tuple, assuming the record is the second element

                for table2 in table2_values:
                    table2_value = table2[1]  # Assuming the record is the second element

                    if self.columns_to_include is not None:
                        filtered_table2_value = {k: v for k, v in table2_value.items() if k in self.columns_to_include}
                    else:
                        filtered_table2_value = table2_value

                    yield {**table1_value, **filtered_table2_value}


class LeftJoinFn(beam.DoFn):
    """
    Implements a left join operation between two datasets, ensuring all records from TABLE1 
    appear in the output, with matched records from TABLE2 where available.
    """
    def __init__(self, columns_to_include=None):
        """
        Initializes the LeftJoinFn instance.
        
        Args:
            columns_to_include (list of str, optional): Columns from TABLE2 to include in the 
            output. Defaults to None, meaning all columns are included.
        """
        self.columns_to_include = columns_to_include

    def process(self, element):
        """
        For each key-grouped element, performs a left join. Outputs each TABLE1 record with 
        matched TABLE2 records merged in based on the common key.
        
        Args:
            element (tuple): The key and the grouped values from both TABLE1 and TABLE2.
        """
        key, grouped_values = element
        table1_values = grouped_values['TABLE1']
        table2_values = grouped_values['TABLE2']

        # Determine the full set of columns to include from TABLE2
        all_columns = self.columns_to_include if self.columns_to_include is not None else None

        for table1 in table1_values:
            table1_value = table1[1]  # Assuming the record is the second element in the tuple

            if table2_values:
                for table2 in table2_values:
                    table2_value = table2[1]
                    if all_columns is not None:
                        # Include only specified columns from TABLE2
                        filtered_table2_value = {k: v for k, v in table2_value.items() if k in all_columns}
                    else:
                        filtered_table2_value = table2_value
                        
                    for k in filtered_table2_value.keys():
                        if filtered_table2_value[k] is None:
                            filtered_table2_value[k] = ''
                    yield {**table1_value, **filtered_table2_value}

            else:
                # Ensure the output format is consistent, even for TABLE1 records with no TABLE2 match
                if all_columns is not None:
                    no_match_table2_value = {k: '' for k in all_columns}
                    yield {**table1_value, **no_match_table2_value}
                else:
                    yield table1_value


class SplitColumnFn(beam.DoFn):
    """
    A DoFn for splitting a column's value into multiple separate parts based on a specified delimiter
    and assigning each part to new columns in the input element. This transformation is useful when a
    single column contains composite data that needs to be separated for further analysis or processing.
    
    The class is flexible to handle any number of output columns up to five and will not break if fewer
    new column names are provided. It also allows specifying a custom delimiter, with '/' as the default.
    
    Attributes:
        column_to_split (str): The name of the column whose value is to be split.
        new_columns (list of str): A list containing the names of the new columns to store the split parts.
        delimiter (str): The delimiter to use for splitting the column's value (default: '/').
    """
    
    def __init__(self, column_to_split, new_columns, delimiter=',', limit_splits=None):
        """
        Initializes the SplitColumnFn with the column to split, the names of the new columns, and the delimiter.
        
        Args:
            column_to_split (str): The name of the column to split.
            new_columns (list of str): The names of the new columns for the split parts.
            delimiter (str, optional): The delimiter to use for splitting the value (default: '/').
        """
        self.column_to_split = column_to_split
        self.new_columns = new_columns
        self.delimiter = delimiter
        self.limit_splits = limit_splits

    def process(self, element):
        """
        Splits the specified column's value using the provided delimiter and assigns the resulting parts
        to new columns in the element. If there are fewer parts than new columns, the remaining new columns
        are set to None. If there are fewer new column names provided than the parts, only the provided
        new columns are populated.
        
        Args:
            element (dict): An element of the PCollection, expected to be a dictionary where the column
                            to split exists.
                            
        Yields:
            The modified element with new columns added for the split parts of the original column's value.
        """
        
        if self.limit_splits is None:
          parts = element.get(self.column_to_split, '').split(self.delimiter)
        else:
          parts = element.get(self.column_to_split, '').split(self.delimiter, self.limit_splits)

        # Assigning split parts to new columns, defaulting to None if the part is not available.
        for i, new_column in enumerate(self.new_columns):
            element[new_column] = parts[i] if i < len(parts) else None

        yield element


class MergeColumnsFn(beam.DoFn):
    def __init__(self, merge_instructions):
        """
        Initializes the MergeColumnsFn with a set of merge instructions.
        Each entry in merge_instructions is a tuple containing:
        - The list of column names to be merged.
        - The name of the new column after merging.
        - An optional delimiter to use in the merge, defaulting to an empty string.

        :param merge_instructions: List of tuples (list of columns to merge, new column name, delimiter)
        """
        self.merge_instructions = merge_instructions

    def process(self, element):
        """
        Processes each element to merge columns based on the initialized merge instructions.
        """
        for columns_to_merge, new_column_name, delimiter in self.merge_instructions:
            # Check if all columns to merge exist in the element
            if all(col in element for col in columns_to_merge):
                # Join the specified columns with the provided delimiter
                merged_value = delimiter.join([str(element[col]) for col in columns_to_merge])
                element[new_column_name] = merged_value
            else:
                # Optionally, handle the case where not all columns are present
                # For example, you could set a default value or log a warning
                element[new_column_name] = 'DEFAULT_VALUE_OR_LOG_WARNING'
        yield element

class GenericDeriveCondition(beam.DoFn):
    def __init__(self, column, map, new_column, default='UNKNOWN'):
        """
        Initializes the DoFn with the necessary parameters.

        Parameters:
        - column: The name of the column to check for categories.
        - map: A dictionary mapping values to descriptions.
        - default: The default description if the column does not exist or the value is not in the map.
        - new_column: The name of the new column that satisfies the map.
        """
        self.column = column
        self.map = map
        self.new_column = new_column
        self.default = default

    def process(self, element):
        # Check if the specified column exists in the element
        if self.column in element:
            # Get the value from the element
            value = element[self.column]

            if value in self.map:
                element[self.new_column] = self.map[value]
            else:
                element[self.new_column] = self.map.get('default', self.default)
                
        else:
            # If the column does not exist, set the description to the default
            element[self.new_column] = self.default

        yield element

#################################################################################################################
# ColumnsToStringConverter ensures that values in specified columns of a PCollection's element 
# (expected to be a dictionary) are converted to strings. This is particularly useful for ensuring data 
# type consistency before writing to file systems or databases that require string inputs.
#################################################################################################################
class ColumnsToStringConverter(beam.DoFn):
    def __init__(self, columns_to_string):
        """
        Initializes the ColumnsToStringConverter instance.
        
        Args:
            columns_to_string (list of str): A list of column names whose values should be converted to strings.
        """
        self.columns_to_string = columns_to_string

    def process(self, element):
        """
        Processes each element, converting specified column values to strings.
        
        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column in self.columns_to_string:
            if column in element:
                try:
                    if element[column] is None or element[column] == "":
                        element[column] = " "
                    else:
                        element[column] = str(element[column])
                except (ValueError, TypeError):
                    pass
        yield element


class ColumnsToFloatConverter(beam.DoFn):
    def __init__(self, columns_to_float):
        """
        Initializes the ColumnsToFloatConverter instance.

        Args:
            columns_to_float (list of str): A list of column names whose values should be converted to floats.
        """
        self.columns_to_float = columns_to_float

    def process(self, element):
        """
        Processes each element, converting specified column values to float, while handling 'None', empty strings, and 'NaN'.

        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column in self.columns_to_float:
            # Ensure the column exists in the element
            if column in element:
                # Handle None, empty string, and 'NaN' by setting them to None
                if element[column] is None or element[column] == "" or str(element[column]).lower() == 'nan':
                    element[column] = None
                else:
                    # Attempt to convert to float, otherwise, leave as is
                    try:
                        element[column] = float(element[column])
                    except ValueError:
                        pass
        yield element


class ColumnsToIntegerConverter(beam.DoFn):
    def __init__(self, columns_to_integer):
        """
        Initializes the ColumnsToIntegerConverter instance.
        
        Args:
            columns_to_integer (list of str): A list of column names whose values should be converted to strings.
        """
        self.columns_to_integer = columns_to_integer

    def process(self, element):
        """
        Processes each element, converting specified column values to integer.
        
        Args:
            element (dict): The input element to process, where keys are column names.
        """
        for column in self.columns_to_integer:
            if column in element and isinstance(element[column], (str, float, Decimal)):
                try:
                  if element[column] == "":
                    element[column] = 0
                  else:
                    element[column] = int(element[column])
                except ValueError:
                  pass
        yield element


def join(table1, table2, method='left_join', columns_to_include=None):
    """Performs a join between two PCollections based on a common key and the specified method.

    Args:
        table1: The first PCollection to join.
        table2: The second PCollection to join.
        method (str): The join method, either 'left_join' or 'inner_join'.
        columns_to_include (list): To include specific columns in join.

    Returns:
        A PCollection resulting from the specified join of table1 and table2.
    """
    if method == 'left_join':
        join_fn = LeftJoinFn(columns_to_include)
    elif method == 'inner_join':
        join_fn = InnerJoinFn(columns_to_include)
    else:
        raise ValueError("Unsupported join method: {}. Use 'left_join' or 'inner_join'.".format(method))

    def apply_join(p_collection):
        result = (p_collection
                  | f"CoGroupByKey {table1} {table2} {method}" >> beam.CoGroupByKey()
                  | f"Apply Join Logic {table1} {table2} {method}" >> beam.ParDo(join_fn))
        return result

    return apply_join({'TABLE1': table1, 'TABLE2': table2})


def key_transform(p_collection, key_columns, identifier=''):
    """Applies a composite key creation and a subsequent key transformation on a PCollection.

    Args:
        p_collection: The input PCollection to transform.
        key_columns (list of str): The columns to use for creating the composite key.

    Returns:
        A PCollection with elements keyed by the specified columns.
    """
    identifier_suffix = f"_{identifier}" if identifier else ""
    return (p_collection
            | f"Create Composite Key {key_columns} on {identifier_suffix}" >> beam.ParDo(KeyByComposite(key_columns))
            | f"Transform Key {key_columns} on {identifier_suffix}" >> beam.ParDo(CreateKeyDoFn(key_columns)))


class AddPeriodColumn(beam.DoFn):
    """
    A custom DoFn class for Apache Beam to dynamically add a new column to each element in a PCollection
    based on the date information from a specified input column. The new column will contain a string
    representing a date format specified by the user, derived from the date in the input column.
    
    This implementation allows specifying the names of the input and output columns dynamically, as well
    as the format of the output date string.
    """
    
    def process(self, element, input_column, output_column, date_format):
        import calendar
        """
        The process method is called on each element of the input PCollection.
        
        Args:
            element: A dictionary representing a single record in the PCollection.
            input_column: The name of the column in 'element' that contains date information.
            output_column: The name of the column to be added to 'element', which will contain the
                           formatted date string.
            date_format: A string representing the desired format of the output date. It should follow
                         the Python strftime format codes.
        
        Yields:
            The same element dictionary with an added column named as specified by 'output_column'. The
            value in this column is a string formatted according to 'date_format', derived from the date
            in 'input_column'.
        """
        # Ensure the specified input column is present in the element.
        if input_column not in element:
            raise ValueError(f"Element must contain an '{input_column}' field")

        # Extract the date object from the specified input column of the element.
        date_obj = element[input_column]
        
        # Use the calendar module to find the last day of the month for the given date.
        last_day = calendar.monthrange(date_obj.year, date_obj.month)[1]
        
        # Create a new date object with the last day of the month.
        last_day_date_obj = date_obj.replace(day=last_day)

        # Format the date according to the specified format.
        formatted_date_str = last_day_date_obj.strftime(date_format)

        # Add the new column to the element with the specified name and the formatted date string.
        element[output_column] = formatted_date_str

        # Yield the modified element back to the pipeline.
        yield element


class ConvertDateFn(beam.DoFn):
    """
    A generic Apache Beam DoFn class for converting date formats within elements of a PCollection.
    
    This class takes the name of the input date column, the expected input date format,
    and the desired output date format as arguments. It attempts to parse dates in the input
    format and convert them to the output format, updating each element accordingly.
    """
    
    def __init__(self, input_column, date_formats):
        """
        Initializes the ConvertDateFn class with specific formatting details.
        
        Args:
            input_column (str): The name of the column containing date strings to be converted.
            input_format (str): The strftime-compatible formatting string of the input dates.
            output_format (str): The strftime-compatible formatting string for the output dates.
        """
        self.input_column = input_column
        self.date_formats = date_formats
    
    def process(self, element):
        from datetime import datetime
        """
        Processes each element of the input PCollection, converting dates from the input
        format to the output format.
        
        Args:
            element: A dictionary representing a single record in the PCollection.
        
        Yields:
            The same dictionary with the date in 'input_column' converted from 'input_format'
            to 'output_format'.
        """
        # Extract the date string from the specified input column.
        date_str = element[self.input_column]
        date_obj = None

        # Attempt to parse the date string according to the specified input format.
        for format in self.date_formats:
            try:
                date_obj = datetime.strptime(date_str, format)
                break
            except ValueError:
                continue
            
        if date_obj is None:
            raise ValueError("Data no formato inválido: {}".format(date_str))
        
        # Update the element with the new date string in the specified input column.
        element[self.input_column] = date_obj

        # Yield the updated element back to the pipeline.
        yield element


class MultiplyColumns(beam.DoFn):
    """
    A custom DoFn class that multiplies the values of specified columns by a given factor.
    
    Attributes:
        columns (list of str): The names of the columns whose values will be multiplied.
        factor (float): The factor by which to multiply the columns' values.
    """
    
    def __init__(self, columns, factor):
        """
        Initializes the MultiplyColumns instance with the target columns and multiplication factor.
        
        Args:
            columns (list of str): The names of the columns to modify.
            factor (float): The multiplication factor.
        """
        self.columns = columns
        self.factor = factor
    
    def process(self, element):
        """
        Processes each element of the input PCollection, multiplying the value of each specified
        column by the predetermined factor.
        
        Args:
            element: A dictionary representing a single record in the PCollection.
        
        Yields:
            The modified element with the specified columns' values multiplied by the factor.
        """
        for column in self.columns:
            # Check if the specified column exists in the element
            if column in element and element[column] != '':
                # Multiply the column value by the factor
                element[column] = float(element[column]) * self.factor
        
        yield element



class GenericArithmeticOperation(beam.DoFn):
    """
    A Beam DoFn class for performing generic operations on columns of a PCollection.
    Operations include arithmetic calculations, replacing missing values, and type conversion.

    """
    
    def __init__(self, operations):
        """
        Initializes the class with a list of operations to be performed.
        operations = [
            {
            'operands': ['COLUMN_1', 'COLUMN_2', 'COLUMN_3'],
            'result_column': 'COLUMN_4',
            'formula': lambda c1, c2, c3: (c1 + c2) / c3 if c3 else 0  # Avoid division by zero
            }
        ]
        """
        self.operations = operations

    def process(self, element):
        """
        Processes each element based on the specified operations.
        """
        for operation in self.operations:
            # Perform arithmetic operation
            operands = operation.get('operands', [])
            result_column = operation.get('result_column')
            formula = operation.get('formula', lambda x: x)  # A lambda function to apply on operands
            
            # Gather operand values, default to 0 if not found or None
            values = [element.get(col, 0) if element.get(col) is not None else 0 for col in operands]
            
            # Apply formula and update element with result
            try:
                element[result_column] = formula(*values)
            except Exception as e:
                element[result_column] = None  # or any default value on error

        yield element


class ReplaceMissingValues(beam.DoFn):
    """
    An Apache Beam DoFn class that replaces missing or None values in a specified column
    with a custom text.
    """
    
    def __init__(self, column_name, replacement_text):
        """
        Initializes the ReplaceMissingValues instance.
        
        Args:
            column_name (str): The name of the column to check for missing values.
            replacement_text (str): The text to use as a replacement for missing values.
        """
        self.column_name = column_name
        self.replacement_text = replacement_text

    def process(self, element):
        """
        Processes each element, replacing missing or None values in the specified column with the custom text.
        
        Args:
            element (dict): The input element to process, where keys are column names.
        """
        # Check if the column is missing or has a None value, and replace it with the custom text.
        if self.column_name not in element or element[self.column_name] is None:
            element[self.column_name] = self.replacement_text

        yield element


class ReplaceMissingValuesDoFn(beam.DoFn):
    def __init__(self, replacements):
        """
        Initialize the DoFn with a dictionary of column replacements.
        :param replacements: A dictionary where keys are column names and values are the values to replace missing entries.
        """
        self.replacements = replacements

    def process(self, element):
        # Iterate over the replacements and apply them as necessary
        for column, value_to_miss in self.replacements.items():
            # Check if the column exists and if its value is considered "missing"
            if column not in element or element[column] in [None, '', float('nan')]:
                element[column] = value_to_miss
        yield element
        

class GenericDeriveConditionComplex(beam.DoFn):
    def __init__(self, conditions, new_column, default='UNKNOWN'):
        """
        Initializes the DoFn with complex conditions.

        Parameters:
        - conditions: A list of dictionaries, each representing a condition (as a lambda function)
          and its corresponding value to assign.
        - new_column: The name of the new column to be added or updated based on the conditions.
        - default: The default value to assign if none of the conditions match.
        conditions = [
            {
            'condition': lambda x: (x['COLUMN_1'] == 'D' and x['COLUMN_2'] in ['0040.2', '0053', '0020.2', '0003.1', '0040.1']) or
                                    (x['COLUMN_1'] == 'M' and x['COLUMN_2'] in ['0054', '0053', '0003.1', '0036.2']),
            'value': 'VARIABLE'
            },
            # You can add more conditions here if needed
        ]
        """
        self.conditions = conditions
        self.new_column = new_column
        self.default = default

    def process(self, element):
        # Evaluate each condition against the element
        for condition in self.conditions:
            if condition['condition'](element):
                # If the condition is true, assign the corresponding value and break
                element[self.new_column] = condition['value']
                break
        else:
            # If no condition matched, assign the default value
            if callable(self.default):
                element[self.new_column] = self.default(element)
            else:
                element[self.new_column] = self.default

        yield element


class ColumnsToDecimalConverter(beam.DoFn):
    """
    A custom Apache Beam transformation to extract decimal values from one or more fields in an element.

    Args:
        columns: A list of column names to be processed.
    """
    def __init__(self, columns):
        self.columns = columns

    def process(self, element):
        """
        Processes an element, extracts decimal values from the specified fields, and replaces them with the extracted values.

        Args:
            element: The element to be processed.
            columns: A list Variable arguments containing the names of the columns to be processed.

        Yields:
            A new element with the extracted decimal values.
        """

        for column in self.columns:
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
      Converts an input value to a Decimal object with 5 decimal places.

      Args:
          input_value: The input value to be converted.

      Returns:
          A Decimal object representing the input value with 5 decimal places.
      """
      if input_string is None or input_string == "" or isinstance(input_string, str):
          return Decimal(0)  # Return 0 if input_value is None

      try:
          # Convert the input value to a Decimal with 5 decimal places
          return Decimal(input_string).quantize(Decimal('0.00000'))
      except (InvalidOperation, ValueError):
          # If conversion to Decimal fails, return 0
          return Decimal(0.0)
      

class ColumnCopy(beam.DoFn):
    def __init__(self, copy_column_name, paste_column_name):
        """
        Initializes the ColumnCopyDoFn.

        Args:
            copy_column_name (str): The name of the column to copy from.
            paste_column_name (str): The name of the column to paste into.
        """
        self.copy_column_name = copy_column_name
        self.paste_column_name = paste_column_name

    def process(self, element):
        """
        Copies the value of one column to another column in the given element.

        Args:
            element (dict): A dictionary representing a row, with column names as keys and values as values.

        Yields:
            dict: A dictionary representing the modified row after copying the column value.
        """
        # Validate input column names
        if self.copy_column_name not in element:
            raise ValueError(f"Column '{self.copy_column_name}' not found in the input data.")
        if self.paste_column_name in element:
            raise ValueError(f"Column '{self.paste_column_name}' already exists in the input data.")

        # Copy the column value
        element[self.paste_column_name] = element[self.copy_column_name]
        
        yield element

class ColumnValueAssignment(beam.DoFn):
    def __init__(self, value, new_column):
        """
        Initializes the DoFn with the necessary parameters.

        Parameters:
        - value: The single value to be assigned to the new column.
        - new_column: The name of the new column.
        """
        self.value = value
        self.new_column = new_column

    def process(self, element):
        """
        Assigns the single value to the new column.

        Args:
        - element (dict): A dictionary representing a row, with column names as keys and values as values.

        Yields:
        - dict: A dictionary representing the modified row after assigning the single value to the new column.
        """
        # Assign the single value to the new column
        element[self.new_column] = self.value
        yield element


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


class ChineseToPinyinDoFn(beam.DoFn):
    def process(self, element):
        from pypinyin import pinyin, Style
        pinyin_element = {}
        for key, value in element.items():
            if isinstance(value, str):  # Assume all values are strings to be converted
                pinyin_list = pinyin(value, style=Style.TONE3)
                pinyin_text = ' '.join([item[0] for item in pinyin_list])
                pinyin_element[key] = pinyin_text
            else:
                pinyin_element[key] = value  # Copy other types of values as-is
        return [pinyin_element]