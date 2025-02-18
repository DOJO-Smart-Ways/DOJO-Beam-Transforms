from abc import ABC, abstractmethod
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.parquetio import ReadAllFromParquet, WriteToParquet
from pipeline_components import data_enrichment as de
from pipeline_components import data_cleaning as dc

class AbstractStructuredTransform(ABC):

    _input_path = None
    _output_path = None
    _output_schema = None

    def __init__(self):
        pass

    @property
    def input_path(self):
        return self._input_path
    
    @input_path.setter
    def input_path(self, value):
        self._input_path = value

    @property
    def output_path(self):
        return self._output_path
    
    @output_path.setter
    def output_path(self, value):
        self._output_path = value

    @property
    def output_schema(self):
        return self._output_schema
    
    @output_schema.setter
    def output_schema(self, value):
        self._output_schema = value

    @classmethod
    def factory(cls, class_name, *args, **kwargs):
        for subclass in cls.__subclasses__():
            if subclass.__name__ == class_name:
                return subclass(*args, **kwargs)
        raise ValueError(f"Class {class_name} not found.")

    def write(self, pipeline, p_coll, identifier):
        if self._output_path is not None and self._output_schema is not None:
            p_coll | f'Pos write to Parquet {identifier}' >> WriteToParquet(file_path_prefix=self._output_path,schema=self._output_schema, file_name_suffix='.parquet')

            return p_coll
        return None

    def transform(self, pipeline, identifier):

        p_coll = None

        print('run ' + identifier)

        if self._input_path is not None:
            p_coll = (
                pipeline
                | f'Find file paths {identifier}' >> fileio.MatchFiles(self._input_path)
                | f'Get file paths {identifier}' >> beam.Map(lambda file_metadata: file_metadata.path)
                | f'Read Parquet files {identifier}' >> ReadAllFromParquet()
            )
        
        if self.getRenameMap() is not None and p_coll is not None:
            p_coll = p_coll | f'Rename Columns {identifier}' >> beam.ParDo(dc.RenameColumns(self.getRenameMap()))

        if self.getFilters() is not None:
            expressions = []
            for condition in self.getFilters():
                key = condition["key"]
                cond = condition["condition"]
                value = condition["value"]

                if (condition["condition"] == 'startswith'):
                    expressions.append(f"record['{key}'].{cond}({value})")
                else:
                    expressions.append(f"record['{key}'] {cond} {value}")
            condition_expression = " and ".join(expressions)

            #print(condition_expression)

            p_coll = p_coll | f'FILTER by dynamic conditions {identifier}' >> beam.Filter(lambda record: record is not None and isinstance(record, dict) and eval(condition_expression))

        if self.getKeepColumns() is not None and p_coll is not None:
            p_coll = p_coll | f'Keep Columns {identifier}' >> beam.ParDo(dc.KeepColumns(self.getKeepColumns()))

        if self.getIntegerFields() is not None and p_coll is not None:
            p_coll = p_coll | f'Convert fields to Int {identifier}' >> beam.ParDo(de.ColumnsToIntegerConverter(self.getIntegerFields()))

        if self.getDecimalFields() is not None and p_coll is not None:
            
            for field in self.getDecimalFields():
                p_coll = p_coll | f'Clear string field {identifier}'+ field >> beam.Map(lambda record: {**record, field: record[field].replace(',', '') if isinstance(record[field], str) else record[field]})
            
            p_coll = p_coll | f'Convert fields to Float {identifier}' >> beam.ParDo(de.ColumnsToFloatConverter(self.getDecimalFields()))
            
        if self.getStringFields() is not None and p_coll is not None:
            p_coll = p_coll | f'Convert fields to string {identifier}' >> beam.ParDo(de.ColumnsToStringConverter(self.getStringFields()))
            p_coll = p_coll | f'Trim fields values {identifier}' >> beam.ParDo(dc.TrimValues(self.getStringFields()))
            
        if self.makeDeduplication():
            p_coll = p_coll | f'Deduplicate dataset {identifier}' >> beam.ParDo(dc.DeduplicateFn())

        if self.getReplaceValues() is not None:
            p_coll = p_coll | f'Replace Values {identifier}' >> beam.ParDo(dc.ReplaceValues(self.getReplaceValues()))

        if self.getChangeDateFormat() is not None:
            for input_date in self.getChangeDateFormat():
                column = input_date["date_column"]
                format = input_date["input_format"]
            p_coll = p_coll | f'Change Date Format {identifier}' >> beam.ParDo(dc.ChangeDateFormat(column, format))

        if self._output_path is not None and self._output_schema is not None:
            p_coll | f'Write to Parquet {identifier}' >> WriteToParquet(file_path_prefix=self._output_path,schema=self._output_schema, file_name_suffix='.parquet')

        return p_coll

    @abstractmethod
    def getDropColumns(self):
        pass

    @abstractmethod
    def getKeepColumns(self):
        pass

    @abstractmethod
    def getRenameMap(self):
        pass

    @abstractmethod
    def getIntegerFields(self):
        pass

    @abstractmethod
    def getStringFields(self):
        pass

    @abstractmethod
    def getDecimalFields(self):
        pass

    @abstractmethod
    def makeDeduplication(self):
        pass

    @abstractmethod
    def getFilters(self):
        pass
    
    @abstractmethod
    def getReplaceValues(self):
        pass

    @abstractmethod
    def getChangeDateFormat(self):
        pass

    