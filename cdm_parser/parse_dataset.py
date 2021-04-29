import csv
import pandas as pd
from postgres_manager import PostgresManager
from cdm_builder import get_observation, get_condition, get_measurement, get_person
from constants import *
from utils import arrays_to_dict
from datetime import datetime

CDM_SQL = {
    CONDITION: get_condition,
    MEASUREMENT: get_measurement,
    OBSERVATION: get_observation
}

def parse_dataset(path, source_mapping, destination_mapping, start, limit, pg):
    """ Parse the dataset to the CDM format.
    """
    print(f'Parse dataset from file {path}')

    value_mapping = create_value_mapping(source_mapping, destination_mapping)
    (date_source_variable, date_format) = get_date_parameters(source_mapping)

    parsing_info = (
        source_mapping,
        destination_mapping,
        value_mapping,
        date_source_variable,
        date_format,
    )
    kwargs = {
        'start': start,
        'limit': limit,
    }

    reader = None
    if '.csv' in path:
        with open(path) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for i in range(start):
                next(csv_reader)
            transform_rows(enumerate(csv_reader, start=start), *parsing_info, pg, **kwargs)
    elif '.sav' in path:
        df = pd.read_spss(path)
        transform_rows(df.loc[start:].iterrows(), *parsing_info, pg, **kwargs)
    elif '.sas' in path:
        df = pd.read_sas(path)
        transform_rows(df.loc[start:].iterrows(), *parsing_info, pg, **kwargs)

def get_date_parameters(source_mapping):
    """ Returns the date source variable and format
    """
    date_source_variable = None
    date_format = None
    if DATE in source_mapping:
        date_source_variable = source_mapping[DATE][SOURCE_VARIABLE]
        date_format = source_mapping[DATE][FORMAT]
    return (date_source_variable, date_format)

def create_value_mapping(source_mapping, destination_mapping):
    """ Create the mapping between the source and destination values for each variable
    """
    value_mapping = {}
    for key, value in source_mapping.items():
        if value[VALUES]:
            if not value[VALUES_PARSED]:
                raise Exception(f'Error in the source mapping for variable {key}')
            source_values = variable_values_to_dict(value[VALUES], value[VALUES_PARSED])
            if destination_mapping[key][VALUES_CONCEPT_ID]:
                # Mapping each value to the concept ID defined
                destination_values = variable_values_to_dict(destination_mapping[key][VALUES], \
                    destination_mapping[key][VALUES_CONCEPT_ID])
                value_mapping[key] = {map_key: destination_values[map_value] \
                    for map_key, map_value in source_values.items()}
                value_mapping[key][VALUE_AS_CONCEPT_ID] = True
            else:
                # Mapping each value to another string
                value_mapping[key] = source_values
                value_mapping[key][VALUE_AS_CONCEPT_ID] = False
    return value_mapping

def variable_values_to_dict(keys, values, separator='/'):
    """ Convert the string representing the variable values map to a dictionary.
    """
    return arrays_to_dict(
        keys.split(separator),
        values.split(separator)
    )

def get_parsed_value(value_mapping, variable, value):
    """ Get the parsed value for a variable
    """
    if variable in value_mapping:
        if str(value) not in value_mapping[variable]:
            raise Exception(f'Variable {variable} is incorrectly mapped: value {value} is not mapped')
        return (value_mapping[variable][VALUE_AS_CONCEPT_ID], value_mapping[variable][str(value)])
    return (False, value)

def valid_row_value(variable, row):
    """ Validate if the value exists and is not null
    """
    return variable in row and row[variable] and not pd.isnull(row[variable])

def transform_rows(iterator, *args, start, limit):
    """ Transform each row in the dataset
    """
    processed_records = 0
    skipped_records = 0
    for index, row in iterator:
        if limit > 0 and index - start >= limit:
            break
        try:
            transform_row(row, *args)
            processed_records += 1
        except Exception as error:
            # TODO: Use a logger and add this information in a file
            print(f'Skipped record {index} due to an error:', error)
            skipped_records += 1
    print(f'Processed {processed_records} records and skipped {skipped_records} records due to errors')

def transform_row(row, source_mapping, destination_mapping, value_mapping, \
    date_source_variable, date_format, pg):
    """ Transform each row and insert in the database.
    """
    sex_source_variable = source_mapping[GENDER][SOURCE_VARIABLE]
    birth_year_source_variable = source_mapping[YEAR_OF_BIRTH][SOURCE_VARIABLE]

    # TODO: Maybe a temporary table for the mapping between person_id and source_person_id
    # in case there are multiple files/tables
    if not all([valid_row_value(var, row) for var in [sex_source_variable, birth_year_source_variable]]):
        raise Exception('Missing required information, the row should contain the year of birth and gender.')

    # Add a new entry for the person/patient
    person_sql = get_person(
        get_parsed_value(value_mapping, sex_source_variable, row[sex_source_variable])[1],
        row[birth_year_source_variable]
    )
    person_id = pg.run_sql(person_sql, returning=True)
    # Parse the date for the observation/measurement/condition if available
    date = '19700101 00:00:00'
    if date_source_variable in row:
        date_parsed = datetime.strptime(row[date_source_variable], date_format)
        date = date_parsed.strftime(DATE_FORMAT)
    # Parse the observations/measurements/conditions
    for key, value in source_mapping.items():
        if key not in destination_mapping:
            print(f'Skipped variable {key} since its not mapped')
        elif destination_mapping[key][DOMAIN] not in CDM_SQL:
            if destination_mapping[key][DOMAIN] != PERSON:
                print(f'Skipped variable {key} since its domain is not currently accepted')
        elif value[SOURCE_VARIABLE] in row and valid_row_value(value[SOURCE_VARIABLE], row):
            domain = destination_mapping[key][DOMAIN]
            source_value = row[value[SOURCE_VARIABLE]]
            (value_as_concept, parsed_value) = get_parsed_value(value_mapping, key, source_value)
            if not value_as_concept or parsed_value != '_':
                named_args = {
                    'source_value': source_value,
                    'date': date
                }
                if value_as_concept:
                    named_args['value_as_concept'] = parsed_value
                else:
                    named_args['value'] = parsed_value
                pg.run_sql(CDM_SQL[domain](person_id, destination_mapping[key], **named_args))
