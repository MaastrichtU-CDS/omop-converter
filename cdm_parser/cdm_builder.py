import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from postgres_manager import PostgresManager
from constants import *

def create_database():
    """ Create the CDM database.
    """
    print(f'Creating the database {os.environ[DB_DATABASE]}')
    with PostgresManager(default_db=True, isolation_level=ISOLATION_LEVEL_AUTOCOMMIT) as pg:
        pg.create_database(os.environ[DB_DATABASE])

def set_schema(pg):
    """ Set the CDM schema for the database.
    """
    print('Setting up the CDM schema')
    pg.execute_file(OMOP_CDM_DDL_PATH)

def insert_vocabulary(pg):
    """ Insert the vocabulary.
    """
    print('Insert the vocabulary')
    for table, vocabulary_file in VOCABULARY_FILES.items():
        print(f'Populating the {table} table')
        pg.copy_from_file(table, f'{os.environ[VOCABULARY_PATH]}/{vocabulary_file}')

def set_constraints(pg):
    """ Set the constraints for the database.
    """
    print('Insert the CDM primary keys and constraints')
    pg.execute_file(OMOP_CDM_PK_PATH)
    pg.execute_file(OMOP_CDM_CONSTRAINTS_PATH)

def create_sequences(pg):
    """ Create the sequences needed.
    """
    print('Create sequences')
    for sequence in [PERSON_SEQUENCE, OBSERVATION_SEQUENCE, MEASUREMENT_SEQUENCE,
        CONDITION_SEQUENCE, CARE_SITE_SEQUENCE]:
        pg.create_sequence(sequence)

def get_person(gender, year_of_birth, cohort_id):
    """ Build the sql statement for a person.
    """
    return """INSERT INTO PERSON (person_id,gender_concept_id,year_of_birth,
        race_concept_id,ethnicity_concept_id,gender_source_concept_id,race_source_concept_id,
        ethnicity_source_concept_id,care_site_id) VALUES (nextval('person_sequence'),{0},{1},0,0,0,0,0,{2})
        RETURNING person_id;
    """.format(gender, year_of_birth, cohort_id if cohort_id else 'NULL')

def get_observation(person_id, field, value='NULL', value_as_concept=0, source_value='NULL', date='19700101 00:00:00'):
    """ Build the sql statement for an observation.
    """
    unit_concept_id = field[UNIT_CONCEPT_ID] if field[UNIT_CONCEPT_ID] else 0
    return """INSERT INTO OBSERVATION (observation_id,person_id,observation_concept_id,observation_datetime,
        observation_type_concept_id,value_as_string,value_as_concept_id,unit_concept_id,observation_source_value,
        observation_source_concept_id,obs_event_field_concept_id) VALUES (nextval('observation_sequence'),{0},{1},
        '{2}', 32879, '{3}',{4},{5},{6}, 0, 0);
    """.format(person_id, field[CONCEPT_ID], date, value, value_as_concept, unit_concept_id, source_value)

def get_measurement(person_id, field, value='NULL', value_as_concept=0, source_value='NULL', date='19700101 00:00:00'):
    """ Build the sql statement for a measurement.
    """
    unit_concept_id = field[UNIT_CONCEPT_ID] if field[UNIT_CONCEPT_ID] else 0
    return """INSERT INTO MEASUREMENT (measurement_id,person_id,measurement_concept_id,measurement_datetime,
        measurement_type_concept_id,value_as_number,value_as_concept_id,unit_concept_id,measurement_source_concept_id,value_source_value)
        VALUES (nextval('measurement_sequence'), {0}, {1}, '{2}', 0, {3}, {4}, {5}, 0, {6})
    """.format(person_id, field[CONCEPT_ID], date, value, value_as_concept, unit_concept_id, source_value)

def get_condition(person_id, field, value='NULL', value_as_concept=0, source_value='NULL', date='19700101 00:00:00'):
    """ Build the sql statement for a condition.
    """
    return """INSERT INTO CONDITION_OCCURRENCE (condition_occurrence_id,person_id,condition_concept_id,
        condition_start_datetime,condition_type_concept_id,condition_status_concept_id,condition_source_value,
        condition_source_concept_id) VALUES (nextval('condition_sequence'),{0},{1},'{2}',0,0,{3},0)
    """.format(person_id, field[CONCEPT_ID], date, source_value)

def get_cohort(cohort_name):
    """ Build the sql statement for a care site.
    """
    return """
        WITH CS AS (INSERT INTO CARE_SITE (care_site_id,care_site_name,place_of_service_concept_id,
            location_id,care_site_source_value,place_of_service_source_value) SELECT nextval('care_site_sequence'),
            '{0}',0,0,'{0}',{1} WHERE NOT EXISTS (SELECT * FROM CARE_SITE WHERE care_site_name='{0}')
            RETURNING care_site_id)
        SELECT care_site_id FROM CS
        UNION
        SELECT care_site_id FROM CARE_SITE WHERE care_site_name='{0}' LIMIT 1
    """.format(cohort_name, 'NULL')
