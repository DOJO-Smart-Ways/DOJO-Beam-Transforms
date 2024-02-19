# input_file.py
import apache_beam as beam
import pandas as pd
import fitz
from apache_beam.io import fileio
import re
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.io.filesystems import FileSystems
import apache_beam as beam
import re
from apache_beam.io import filesystems

import apache_beam as beam
import re
from apache_beam.io import filesystems

class ReadFilesWithPrefix(beam.DoFn):
    """
    A custom DoFn to read and process CSV files that match a specific prefix pattern, provided as an input.
    It reads the content of each file, assuming a CSV format, and outputs each row as a dictionary.
    
    Attributes:
        prefix (str): The file prefix to match. This prefix is included in the regex pattern for file selection.
    """
    def __init__(self, prefix, delimiter=';'):
        """
        Initializes the ReadFilesWithPrefix instance with a specific file prefix.
        
        Args:
            prefix (str): The prefix string to match in the file names.
        """
        self.prefix = prefix
        self.delimiter = delimiter

    def process(self, file_path, delimiter=';'):
        # Create a regex pattern dynamically based on the provided prefix.
        # This pattern checks if the file path ends with the specified prefix pattern and ".csv".
        pattern = rf'.*/{re.escape(self.prefix)}.*\.csv'
        
        # Check if the file path matches the expected pattern for files with the given prefix.
        if re.match(pattern, file_path):
            # Open the file for reading. Note: 'beam.io.filesystems.FileSystems.open' is used for compatibility
            # with different filesystems (local, GCS, etc.).
            with beam.io.filesystems.FileSystems.open(file_path) as f:
                # Read the entire file content and decode it from bytes to a string.
                lines = f.read().decode('utf-8').strip().split('\n')
                
                # The first line is assumed to contain the column headers, separated by ';'.
                columns = lines[0].split(delimiter)
                
                # Iterate over each line after the header, creating a dictionary for each row
                # where the keys are column names and the values are the corresponding row values.
                for line in lines[1:]:
                    # Use 'zip' to pair each column name with its corresponding value in the current line,
                    # and create a dictionary out of these pairs.
                    yield dict(zip(columns, line.split(delimiter)))

def read_csvs_union(pipeline, input_pattern, delimiter=';'):
    """
    Constructs a pipeline for processing multiple CSV files, adding a 'PERIOD' column to each, and
    consolidating the results into a single PCollection.
    
    Args:
        pipeline: The Beam Pipeline object.
        input_pattern: A glob pattern string to match the input CSV files. E.g. File Name => File - 28.02.2022.csv
        delimiter: The delimiter used in the CSV files.
        
    Returns:
        A PCollection containing records from all processed files, each record being a dictionary.
    """
    return (
        pipeline
        | 'Match Files' >> MatchFiles(input_pattern)  # Match files based on the provided glob pattern
        | 'Read Matches Files CSVs' >> ReadMatches()  # Read matched files
        | 'Process CSVs Files' >> beam.ParDo(ReadFilesWithPrefix(prefix='Employee database',delimiter=delimiter))  # Process each file, adding 'PERIOD'
    )

class ApplyHeadersFn(beam.DoFn):
    def process(self, data_product, headers):
        if len(headers) == len(data_product):
            result = {headers[i]:data_product[i] for i in range(len(headers))}
            yield result
        else:
            yield "Error: Mismatched lengths of headers and data"
            
def read_and_apply_headers(pipeline, input_header, input_file_csv, delimiter=';'):
    """
    Reads headers from a file and applies them to the data read from another file.
    
    Args:
        pipeline: The Apache Beam Pipeline object.
        input_header (str): The file path to read the headers from.
        input_file_csv (str): The file path to read the data from.
    
    Returns:
        A PCollection where each element is a dictionary with headers applied to the data.
    """
    # Read and process headers
    headers = (pipeline
                | 'ReadHeaderPS' >> beam.io.ReadFromText(input_header, skip_header_lines=0, coder=beam.coders.coders.BytesCoder())
                | 'DecodeAndSplitHeaders' >> beam.Map(lambda bytes_line: bytes_line.decode('iso-8859-1').split(';')))

    # Read and process input data
    body = (pipeline
                | 'ReadDadProductSelector' >> beam.io.ReadFromText(input_file_csv, skip_header_lines=0, coder=beam.coders.coders.BytesCoder())
                | 'DecodeBytes' >> beam.Map(lambda bytes_line: bytes_line.decode('iso-8859-1'))
                | 'SplitColumns' >> beam.Map(lambda line: line.split(delimiter)))  # Assuming the splitting logic is simple

    return (body | 'ApplyHeaders' >> beam.ParDo(ApplyHeadersFn(), beam.pvalue.AsSingleton(headers)))


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

def read_bq(pipeline, query, temp_location, use_standard_sql=True):
    return (
        pipeline 
        | 'Execute SQL Query' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=use_standard_sql, gcs_location=temp_location)
    )
