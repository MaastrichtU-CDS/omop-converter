from constants import *
from exceptions import ParsingError
from parse_dataset import DataParser

TYPE_INT = 'int'
TYPE_NUMERIC = 'num'
TYPE_TEXT = 'text'
TYPE_BOOL = 'bool'

SQL_INTEGER = 'INTEGER'
SQL_NUMERIC = 'NUMERIC'
SQL_VARCHAR = 'VARCHAR ( 50 )'
SQL_DATE = 'DATE'
SQL_BOOLEAN = 'BOOLEAN'

COLUMN_TYPE = {
    DATE: SQL_DATE,
    TYPE_INT: SQL_INTEGER,
    TYPE_NUMERIC: SQL_NUMERIC,
    TYPE_TEXT: SQL_VARCHAR,
    TYPE_BOOL: SQL_BOOLEAN,
    PERSON: SQL_INTEGER,
    OBSERVATION: SQL_VARCHAR,
    MEASUREMENT: SQL_NUMERIC,
    CONDITION_OCCURRENCE: SQL_INTEGER,
    NOT_APPLICABLE: SQL_VARCHAR
}

def get_parsed_value(mapping, value):
    """ Get the parsed value for a variable.
    """
    if len(mapping.keys()) > 0:
        if str(value) in mapping:
            if mapping[str(value)] == DEFAULT_SKIP:
                return ''
            else:
                return mapping[str(value)]
        raise ParsingError('Error parsing the values for the NCDC plane table')
    return value

def get_column_statement(column_name, type, domain=OBSERVATION):
    """ Build the sql statement for the column.
    """
    return f'{column_name} {COLUMN_TYPE[type] if type else COLUMN_TYPE[domain]}'

def parse_mapping_to_columns(mapping):
    """ Parse a CDM mapping to SQL columns.
    """
    columns = {
        'id': 'id bigint'
    }
    for key, value in mapping.items():
        if key not in columns and key not in [SOURCE_ID] and 'no_' not in key:
            columns[key] = get_column_statement(key, value[TYPE], value[DOMAIN])
        if value[DATE] and value[DATE] not in columns:
            columns[value[DATE]] = get_column_statement(value[DATE], DATE)
    columns['pk'] = 'PRIMARY KEY (id, date)'
    return columns

def parse_visit(destination_mapping, columns, visit, observations, measurements, conditions):
    """ Parse the CDM entries to a row for the plane table.
    """
    concept_mapping = {}
    column_value = {}
    for key in columns:
        if key in destination_mapping:
            column_value[key] = ''
            concept_id = destination_mapping[key][CONCEPT_ID] or key
            if concept_id and concept_id not in concept_mapping:
                concept_mapping[concept_id] = destination_mapping[key]
                concept_mapping[concept_id][VARIABLE] = key
                concept_mapping[concept_id]['mapping'] = {}
                if concept_mapping[concept_id][NCDC_MAP] and (concept_mapping[concept_id][VALUES_CONCEPT_ID] or \
                    concept_mapping[concept_id][VALUES]):
                    concept_mapping[concept_id]['mapping'] = DataParser.variable_values_to_dict(
                        concept_mapping[concept_id][VALUES_CONCEPT_ID] or concept_mapping[concept_id][VALUES],
                        concept_mapping[concept_id][NCDC_MAP]
                    )

    # Person information
    column_value[ID] = visit[2]
    column_value[DATE] = visit[1].strftime('%Y-%m-%d')
    column_value[YEAR_OF_BIRTH] = visit[3]
    column_value[GENDER] = get_parsed_value(concept_mapping[GENDER]['mapping'], visit[4])
    column_value[DEATH_DATE] = visit[5].strftime('%Y/%m/%d') if visit[5] and visit[5].strftime('%d/%m/%Y') != '01/01/1970' else ''
    column_value[DEATH_FLAG] = 1 if visit[5] else 0
    # Observations
    for observation in observations:
        concept_map = concept_mapping[str(observation[0])]
        if concept_map[DOMAIN] == OBSERVATION:
            column_value[concept_map[VARIABLE]] = get_parsed_value(concept_map['mapping'], observation[3]) or observation[2]
        elif concept_map[DOMAIN] == CONDITION_OCCURRENCE:
            column_value[concept_map[VARIABLE]] = 0
        else:
            raise ParsingError(
                f'Error while creating the plane table columns for variable: {concept_map[VARIABLE]}')
        if concept_map[DATE] and observation[1] and observation[1].strftime('%d/%m/%Y') != '01/01/1970':
            column_value[concept_map[DATE]] = observation[1].strftime('%Y-%m-%d')
    # Measurements
    for measurement in measurements:
        concept_map = concept_mapping[str(measurement[0])]
        if concept_map[DATE] and measurement[1] and measurement[1].strftime('%d/%m/%Y') != '01/01/1970':
            column_value[concept_map[DATE]] = measurement[1].strftime('%Y-%m-%d')
        column_value[concept_map[VARIABLE]] = get_parsed_value(concept_map['mapping'], measurement[2])
    # Condition Occurrence
    for condition in conditions:
        concept_map = concept_mapping[str(condition[0])]
        if concept_map[DATE] and condition[1] and condition[1].strftime('%d/%m/%Y') != '01/01/1970':
            column_value[concept_map[DATE]] = condition[1].strftime('%Y-%m-%d')
        column_value[concept_map[VARIABLE]] = 1
    return column_value
