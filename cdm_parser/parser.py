import csv
from constants import *

def parse_csv_mapping(file, delimiter=','):
    """ Parse a CSV mapping that follows the specifications.
    """
    print(f'Parsing file {file}')
    mapping = {}
    with open(file) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
        for row in csv_reader:
            mapping[row[VARIABLE]] = row
    return mapping
