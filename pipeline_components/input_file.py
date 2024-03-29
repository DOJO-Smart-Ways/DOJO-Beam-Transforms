# input_file.py
import apache_beam as beam


class ProcessCSVFiles(beam.DoFn):
    def __init__(self, delimiter=';'):
        self.delimiter = delimiter

    def process(self, file):
        from apache_beam.io.filesystems import FileSystems
        import pandas as pd
        import re
        # Extract date from file name using regex
        match = re.search(r'(\d{2}\.\d{2}\.\d{4})', file.metadata.path)
        if match:
            period = match.group(1)
        else:
            period = 'Unknown'

        # Read CSV file
        with FileSystems.open(file.metadata.path) as f:
            df = pd.read_csv(f, delimiter=self.delimiter, engine='python')

        # Add PERIOD column with extracted date
        df['PERIOD'] = period

        # Return records as dictionaries
        yield from df.to_dict('records')

def read_csvs_union(pipeline, input_pattern, delimiter=';', identifier=''):
    from apache_beam.io.fileio import MatchFiles, ReadMatches
    identifier_suffix = f"_{identifier}" if identifier else ""
    return (
        pipeline
        | f'Match Files {identifier_suffix}' >> MatchFiles(input_pattern)
        | f'Read Matches {identifier_suffix}' >> ReadMatches()
        | f'Process CSV Files {identifier_suffix}' >> beam.ParDo(ProcessCSVFiles(delimiter=delimiter))
    )

class ProcessExcelFiles(beam.DoFn):
    def __init__(self, skip_rows=None, tab=None):
        self.skip_rows = skip_rows
        self.tab = 0 if tab is None else tab

    def process(self, file):
        from apache_beam.io.filesystems import FileSystems
        import pandas as pd
        # Read Excel file, assuming it's the first sheet; adjust if necessary
        with FileSystems.open(file.metadata.path) as f:
            df = pd.read_excel(f, skiprows=self.skip_rows, sheet_name=self.tab)

        df['SOURCE'] = file.metadata.path
        # Return records as dictionaries
        yield from df.to_dict('records')

def read_excels_union(pipeline, input_pattern, identifier='', skiprows=None, tab=None):
    from apache_beam.io.fileio import MatchFiles, ReadMatches
    identifier_suffix = f"_{identifier}" if identifier else ""
    return (
        pipeline
        | f'Match Files {identifier_suffix}' >> MatchFiles(input_pattern)
        | f'Read Matches {identifier_suffix}' >> ReadMatches()
        | f'Process Excel Files {identifier_suffix}' >> beam.ParDo(ProcessExcelFiles(skip_rows=skiprows, tab=tab))
    )

class ApplyHeadersFn(beam.DoFn):
    def process(self, data_product, headers):
        if len(headers) == len(data_product):
            result = {headers[i]:data_product[i] for i in range(len(headers))}
            yield result
        else:
            yield "Error: Mismatched lengths of headers and data"
            
def read_and_apply_headers(pipeline, input_header, input_file_csv, delimiter=';', identifier=''):
    """
    Reads headers from a file and applies them to the data read from another file.
    
    Args:
        pipeline: The Apache Beam Pipeline object.
        input_header (str): The file path to read the headers from.
        input_file_csv (str): The file path to read the data from.
    
    Returns:
        A PCollection where each element is a dictionary with headers applied to the data.
    """
	
    identifier_suffix = f"_{identifier}" if identifier else ""
    # Read and process headers
    headers = (pipeline
                | f'ReadHeaderPS {identifier_suffix}' >> beam.io.ReadFromText(input_header, skip_header_lines=0, coder=beam.coders.coders.BytesCoder())
                | f'DecodeAndSplitHeaders {identifier_suffix}' >> beam.Map(lambda bytes_line: bytes_line.decode('iso-8859-1').split(';')))

    # Read and process input data
    body = (pipeline
                | f'ReadDadProductSelector {identifier_suffix}' >> beam.io.ReadFromText(input_file_csv, skip_header_lines=0, coder=beam.coders.coders.BytesCoder())
                | f'DecodeBytes {identifier_suffix}' >> beam.Map(lambda bytes_line: bytes_line.decode('iso-8859-1'))
                | f'SplitColumns {identifier_suffix}' >> beam.Map(lambda line: line.split(delimiter)))  # Assuming the splitting logic is simple

    return (body | f'ApplyHeaders {identifier_suffix}' >> beam.ParDo(ApplyHeadersFn(), beam.pvalue.AsSingleton(headers)))


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
                        "EMPLOYEE_ID": match[1],
                        "FIRST_NAME": match[2],
                        "LAST_NAME": match[3],
                        "COST_CENTRE": match[5],
                        "DEPARTMENT": match[6].strip() if match[6] else "",
                        "ENDING_PAYMENT": '',
                        "DIMISSAL_DATE": match[4],
                        "GROUP": match[7]
                    }
                    yield row_data

def read_pdf(pipeline, input_file, identifier=''):
    from apache_beam.io import fileio
    identifier_suffix = f"_{identifier}" if identifier else ""
    return (pipeline
        | f'Match PDF Files {identifier_suffix}' >> fileio.MatchFiles(input_file)
        | f'Read Matches {identifier_suffix}' >> fileio.ReadMatches()
        | f'Process PDFs {identifier_suffix}' >> beam.ParDo(ProcessPDF())
    )

def read_csv(pipeline, input_file, delimiter=';', identifier=''):
    import pandas as pd
    identifier_suffix = f"_{identifier}" if identifier else ""
    return (
        pipeline
        | f'Create File Path Excel {identifier_suffix}' >> beam.Create([input_file])
        | f'Read Excel {identifier_suffix}' >> beam.FlatMap(lambda file: pd.read_csv(file, delimiter=delimiter, engine='python').to_dict('records'))
    )


def read_txt(pipeline, input_file, skip_header_lines=0, identifier=''):
    identifier_suffix = f"_{identifier}" if identifier else ""
    return (
        pipeline
        | f'Read Txt {identifier_suffix}' >> beam.io.ReadFromText(input_file, skip_header_lines=skip_header_lines, coder=beam.coders.coders.BytesCoder())
        | f'DecodeBytes {identifier_suffix}' >> beam.Map(lambda bytes_line: bytes_line.decode('iso-8859-1'))
    )

def read_bq(pipeline, query, temp_location, use_standard_sql=True, identifier=''):
    identifier_suffix = f"_{identifier}" if identifier else ""
    return (
        pipeline 
        | f'Execute SQL Query {identifier_suffix}' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=use_standard_sql, gcs_location=temp_location)
    )

def read_excel_transpose_and_dict(pipeline, input_file, tab, header_row=1):
    import pandas as pd
    """
	Reads an Excel file, transposes the data, skips the initial rows, and converts it to a dictionary format.
	
	Args:
	pipeline: The Apache Beam pipeline object.
	input_file: The path to the input Excel file.
	tab: The name or index of the sheet to read from the Excel file.
	skip_rows: Number of initial rows to skip before setting the new header.
	
	Returns:
	A PCollection containing dictionaries, where each dictionary represents a row
	from the transposed Excel sheet, with the row following the skipped rows used
	as dictionary keys.
	"""
    def transpose_and_convert_to_dict(file):
		# Read the specified tab from the Excel file into a DataFrame.
        df = pd.read_excel(file, tab)
		
		# Transpose the DataFrame.
        transposed_df = df.transpose()
		
		# Determine the new header based on the `skip_rows` parameter.
		# This uses the row at the position `skip_rows` as the new header.
        new_header = transposed_df.iloc[header_row]  
		
		# Adjust the DataFrame to start after the new header row,
		# effectively skipping the desired number of rows before setting the header.
        transposed_df = transposed_df[(header_row + 1):] 
		
		# Assign the new headers to the DataFrame.
        transposed_df.columns = new_header  # Set new header
        transposed_df.reset_index(drop=True, inplace=True)  # Optionally reset index
		
		# Convert the adjusted DataFrame to a list of dictionaries
        return transposed_df.to_dict('records')
		
    return (
        pipeline
        | 'Create File Path' >> beam.Create([input_file])
        | 'Read and Transpose CSV' >> beam.FlatMap(transpose_and_convert_to_dict)
    )


class ProcessExcelFiles(beam.DoFn):
    def __init__(self, skip_rows=None, tab=None):
        self.skip_rows = skip_rows
        self.tab = 0 if tab is None else tab

    def process(self, file):
        import pandas as pd
        from apache_beam.io.filesystems import FileSystems
        # Read Excel file, assuming it's the first sheet; adjust if necessary
        with FileSystems.open(file.metadata.path) as f:
            df = pd.read_excel(f, skiprows=self.skip_rows, sheet_name=self.tab, dtype=str)

        # If tab is an integer, fetch the actual sheet name for inclusion in SOURCE
        if isinstance(self.tab, int):
            xls = pd.ExcelFile(file.metadata.path)
            sheet_name = xls.sheet_names[self.tab]
        else:
            sheet_name = self.tab

        # Append the file path and sheet name to 'SOURCE'
        df['SOURCE'] = f"{file.metadata.path}/{sheet_name}"

        # Return records as dictionaries
        yield from df.to_dict('records')

def read_excels_union(pipeline, input_pattern, identifier='', skiprows=None, tab=None):
    from apache_beam.io.fileio import MatchFiles, ReadMatches
    identifier_suffix = f"_{identifier}" if identifier else ""
    return (
        pipeline
        | f'Match Files {identifier_suffix}' >> MatchFiles(input_pattern)
        | f'Read Matches {identifier_suffix}' >> ReadMatches()
        | f'Process Excel Files {identifier_suffix}' >> beam.ParDo(ProcessExcelFiles(skip_rows=skiprows, tab=tab))
    )