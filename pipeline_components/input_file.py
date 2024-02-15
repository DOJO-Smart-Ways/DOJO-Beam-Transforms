# beam_transforms.py
import apache_beam as beam
import pandas as pd
import fitz

class ReadPDFDoFn(beam.DoFn):
    def process(self, element):
        file_path = element
        try:
            with fitz.open(file_path) as doc:
                text = []
                for page in doc:
                    text.append(page.get_text())
            yield {'file_path': file_path, 'text': text}
        except Exception as e:
            yield beam.pvalue.TaggedOutput('failed', (file_path, str(e)))

def read_csv(pipeline, input_file, delimiter=';'):
    return (
        pipeline
        | 'Create File Path Excel' >> beam.Create([input_file])
        | 'Read Excel -> Origin' >> beam.FlatMap(lambda file: pd.read_csv(file, delimiter=delimiter, engine='python').to_dict('records'))
    )

def read_txt(pipeline, input_file, skip_header_lines=0):
    return (
        pipeline
        | 'ReadDadProductSelector' >> beam.io.ReadFromText(input_file, skip_header_lines=skip_header_lines, coder=beam.coders.coders.BytesCoder())
        | 'DecodeBytes' >> beam.Map(lambda bytes_line: bytes_line.decode('iso-8859-1'))
    )

def read_pdf(pipeline, input_file):
    return (
        pipeline
        | 'Create File Path PDF' >> beam.Create([input_file])
        | 'Read PDF -> Origin' >> beam.ParDo(ReadPDFDoFn()).with_outputs('failed', main='text')
    )
