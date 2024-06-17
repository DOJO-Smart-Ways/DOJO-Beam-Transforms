from typing import List, Tuple, Dict

def build_gcs_path(*args: str) -> str:
    """
    Constructs a Google Cloud Storage (GCS) path from the provided arguments.

    This function takes multiple string arguments and constructs a GCS path in the
    format 'gs://<arg1>/<arg2>/.../<argN>'. If no arguments are provided, it raises
    a ValueError.

    Args:
        *args: A variable length argument list of strings representing the components
               of the GCS path.

    Returns:
        str: A string representing the GCS path.

    Raises:
        ValueError: If no arguments are provided.

    Examples:
        >>> build_gcs_path("bucket", "path", "to", "file")
        'gs://bucket/path/to/file'

        >>> build_gcs_path("my_bucket", "my_folder")
        'gs://my_bucket/my_folder'

        >>> build_gcs_path()
        Traceback (most recent call last):
          ...
        ValueError: At least one argument must be provided.
    """
    if not args:
        raise ValueError("At least one argument must be provided.")
    return "gs://" + "/".join(args)



def build_bq_schema(column_tuples: List[Tuple[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    """
    Builds a BigQuery schema from a list of tuples containing column name and data type.

    This function takes a list of tuples where each tuple consists of a column name and
    its corresponding data type, and constructs a BigQuery schema in dictionary format.

    Args:
        column_tuples (List[Tuple[str, str]]): A list of tuples, where each tuple contains
                                               the column name as a string and the data type
                                               as a string.

    Returns:
        Dict[str, List[Dict[str, str]]]: A dictionary representing the BigQuery schema.

    Raises:
        ValueError: If column_tuples is empty, contains invalid tuples, or contains
                    unsupported BigQuery data types.

    Examples:
        >>> build_bq_schema([("name", "STRING"), ("age", "INT64")])
        {'fields': [{'name': 'name', 'type': 'STRING'}, {'name': 'age', 'type': 'INT64'}]}

        >>> build_bq_schema([("id", "INT64"), ("value", "FLOAT64")])
        {'fields': [{'name': 'id', 'type': 'INT64'}, {'name': 'value', 'type': 'FLOAT64'}]}

        >>> build_bq_schema([])
        Traceback (most recent call last):
          ...
        ValueError: Column tuples list must not be empty.
    """
    valid_types = {
        "ARRAY", "BOOL", "BYTES", "DATE", "DATETIME", "GEOGRAPHY", "INTERVAL", "JSON",
        "INT64", "INT", "SMALLINT", "INTEGER", "BIGINT", "TINYINT", "BYTEINT", "NUMERIC",
        "DECIMAL", "BIGNUMERIC", "BIGDECIMAL", "FLOAT64", "FLOAT","RANGE", "STRING", "STRUCT",
        "TIME", "TIMESTAMP"
    }
    
    if not column_tuples:
        raise ValueError("Column tuples list must not be empty.")
    
    schema = {'fields': []}
    for name, data_type in column_tuples:
        if not isinstance(name, str) or not isinstance(data_type, str):
            raise ValueError("Each tuple must contain a column name and data type as strings.")
        if data_type.upper() not in valid_types:
            raise ValueError(f"Invalid data type '{data_type}' in column '{name}'. Supported types are: {', '.join(sorted(valid_types))}")
        schema['fields'].append({'name': name, 'type': data_type.upper()})
    
    return schema
