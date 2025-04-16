import csv
import os


def read_csv(file_path):
    """
    Reads a CSV file and returns a list of dictionaries, where each dictionary represents a row.

    Parameters:
    - file_path: The path to the CSV file.

    Returns:
    - A list of dictionaries containing the CSV data.
    """
    full_path = os.path.abspath('unit_tests/data/' + file_path)
    with open(full_path, mode='r') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]
