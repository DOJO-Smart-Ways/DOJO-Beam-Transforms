# input_file.py
import apache_beam as beam
import pandas as pd
import fitz
from apache_beam.io import fileio


class ProcessPDF(beam.DoFn):
    def process(self, readable_file):
        import fitz  # PyMuPDF
        import re
        row_pattern = re.compile(
            r'(\d+)\s+(\d+)\s+([\w\s-]+?)\s+([\w\s-]+?)\s+(\d+\.\d+\.\d+)\s+([A-Z\d]+)\s*([\w\s]+?)\s*([D|W])'
        )

        with readable_file.open() as file_handle:
            doc = fitz.open(stream=file_handle.read(), filetype="pdf")
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                text = page.get_text()

                # Once the table start is identified, extract rows
                matches = row_pattern.findall(text)
                for match in matches:
                    row_data = {
                        " ":match[0],
                        "EMPLOYEE_ID": match[1],
                        "FIRST_NAME": match[2],
                        "LAST_NAME": match[3],
                        "COST_CENTRE": match[5],
                        "DEPARTAMENT": match[6].strip() if match[6] else "",
                        "ENDING_PAYMENT": '',
                        "DIMISSAL_DATE": match[4],
                        "GROUP": match[7]
                    }
                    yield row_data

def read_pdf(pipeline, input_file):
    return (pipeline
        | 'Match PDF Files' >> fileio.MatchFiles(input_file)
        | 'Read Matches' >> fileio.ReadMatches()
        | 'Process PDFs' >> beam.ParDo(ProcessPDF())
    )

def read_csv(pipeline, input_file, delimiter=';'):
    return (
        pipeline
        | 'Create File Path Excel' >> beam.Create([input_file])
        | 'Read Excel' >> beam.FlatMap(lambda file: pd.read_csv(file, delimiter=delimiter, engine='python').to_dict('records'))
    )


def read_txt(pipeline, input_file, skip_header_lines=0):
    return (
        pipeline
        | 'Read Txt' >> beam.io.ReadFromText(input_file, skip_header_lines=skip_header_lines, coder=beam.coders.coders.BytesCoder())
        | 'DecodeBytes' >> beam.Map(lambda bytes_line: bytes_line.decode('iso-8859-1'))
    )

def read_header_from_csv(pipeline, input_file, skip_header_lines=0):
    return (
        pipeline
        | 'ReadHeaderPS' >> beam.io.ReadFromText(input_file_header, skip_header_lines=0, coder=beam.coders.coders.BytesCoder())
        | 'DecodeAndSplitHeaders' >> beam.Map(lambda bytes_line: bytes_line.decode('iso-8859-1').split(';'))
    )
