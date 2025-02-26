import apache_beam as beam

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
