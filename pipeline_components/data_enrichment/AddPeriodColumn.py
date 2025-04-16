import apache_beam as beam

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