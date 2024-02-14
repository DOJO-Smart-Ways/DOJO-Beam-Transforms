import apache_beam as beam


#################################################################################################################
#  Rename Column E.g. 'DOJO' to 'DOJO_WINNER'
#################################################################################################################
class RenameColumns(beam.DoFn):
    def __init__(self, column_mapping):
        self.column_mapping = column_mapping

    def process(self, element):
        new_element = {self.column_mapping.get(k, k): v for k, v in element.items()}
        yield new_element

#################################################################################################################
#  Replace Values E.g. ',' to ';'
#################################################################################################################
class ReplaceValues(beam.DoFn):
    def __init__(self, replacements):
        self.replacements = replacements

    def process(self, element):
        for column, current_value, replacement  in self.replacements:
            if column in element:
                column_value = element[column]
            if isinstance(column_value, str) and current_value in column_value:
                element[column] = column_value.replace(current_value, replacement)
        yield element


#################################################################################################################
#  Replace Values E.g. ',' to ';'
#################################################################################################################
class ColumnsToStringConverter(beam.DoFn):
    def __init__(self, columns_to_string):
        self.columns_to_string = columns_to_string

    def process(self, element):
        for column in self.columns_to_string:
           if column in element:
                try:
                  if element[column] is None:
                    element[column] = " "
                  elif element[column] == "":
                    element[column] = " "
                  else:
                    element[column] = str(element[column])
                except (ValueError, TypeError):
                  pass
        yield element
