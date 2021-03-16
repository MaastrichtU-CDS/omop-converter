import csv
import pandas as pd
from postgres_manager import PostgresManager
from cdm_builder import get_observation, get_condition, get_measurement, get_person
from constants import *
from utils import arrays_to_dict

CDM_SQL = {
    CONDITION: get_condition,
    MEASUREMENT: get_measurement,
    OBSERVATION: get_observation
}

def parse_dataset(path, source_mapping, destination_mapping, pg):
    """ Parse the dataset to the CDM format.
    """
    print(f'Parse dataset from file {path}')
    if 'csv' in path:
        with open('../examples/dataset.csv') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            value_mapping = create_value_mapping(source_mapping, destination_mapping)
            for row in csv_reader:
                transform_row(row, source_mapping, destination_mapping, value_mapping, pg)
    elif 'sav' in path:
        df = pd.read_spss(path)
        for index, row in df.iterrows():
            transform_row(row, source_mapping, destination_mapping, pg)

def create_value_mapping(source_mapping, destination_mapping):
    """ Create the mapping between the source and destination values for each variable
    """
    value_mapping = {}
    for key, value in source_mapping.items():
        if value[VALUES]:
            if not value[VALUES_PARSED]:
                raise Exception(f'Error in the source mapping for variable {key}')
            elif not destination_mapping[key][VALUES]:
                raise Exception(f'Error in the destination mapping for variable {key}')
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
        if value not in value_mapping[variable]:
            raise Exception(f'Variable {variable} is incorrectly mapped')
        return (value_mapping[variable][VALUE_AS_CONCEPT_ID], value_mapping[variable][value])
    return (False, value)

def transform_row(row, source_mapping, destination_mapping, value_mapping, pg):
    """ Transform each row and insert in the database.
    """
    sex_source_variable = source_mapping[GENDER][SOURCE_VARIABLE]
    birth_year_source_variable = source_mapping[YEAR_OF_BIRTH][SOURCE_VARIABLE]

    # TODO: Maybe a temporary table for the mapping between person_id and source_person_id
    # in case there are multiple files/tables
    person_sql = get_person(
        get_parsed_value(value_mapping, sex_source_variable, row[sex_source_variable])[1],
        row[birth_year_source_variable]
    )
    person_id = pg.run_sql(person_sql, returning=True)

    for key, value in source_mapping.items():
        if key not in destination_mapping:
            print(f'Skipped variable {key} since its not mapped')
        elif destination_mapping[key][DOMAIN] not in CDM_SQL:
            if destination_mapping[key][DOMAIN] != PERSON:
                print(f'Skipped variable {key} since its domain is not currently accepted')
        elif value[SOURCE_VARIABLE] in row:
            domain = destination_mapping[key][DOMAIN]
            source_value = row[value[SOURCE_VARIABLE]]
            (value_as_concept, parsed_value) = get_parsed_value(value_mapping, key, source_value)
            if value_as_concept:
                statement = CDM_SQL[domain](
                    person_id,
                    destination_mapping[key],
                    source_value=source_value,
                    value_as_concept=parsed_value
                )
            else:
                statement = CDM_SQL[domain](
                    person_id,
                    destination_mapping[key],
                    value=parsed_value,
                    source_value=source_value
                )
            pg.run_sql(statement)
