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
        CONDITION_SEQUENCE, CARE_SITE_SEQUENCE, VISIT_OCCURRENCE, LOCATION_SEQUENCE]:
        pg.create_sequence(sequence)

def create_temp_id_table(pg):
    """ Create temporary table to store the link between person id and the source id.
    """
    print(f'Create temporary id table: {TEMP_ID_TABLE}')
    pg.run_sql(f'CREATE TABLE IF NOT EXISTS {TEMP_ID_TABLE} (person_id bigint PRIMARY KEY, source_id varchar(100) NOT NULL)')

def get_person_id(source_id, pg):
    """ Retrieve the person id from the source id.
    """
    return pg.run_sql(
        f"SELECT person_id FROM {TEMP_ID_TABLE} WHERE source_id='{source_id}' LIMIT 1",
        returning=True,
    )

def insert_temp_id_record(source_id, person_id, pg):
    """ Insert a new record in the temporary table.
    """
    return pg.run_sql(f"INSERT INTO {TEMP_ID_TABLE} VALUES ({person_id}, '{source_id}')")

def build_person(gender, year_of_birth, cohort_id, death_datetime):
    """ Build the sql statement for a person.
    """
    return """INSERT INTO PERSON (person_id,gender_concept_id,year_of_birth,death_datetime,
        race_concept_id,ethnicity_concept_id,gender_source_concept_id,race_source_concept_id,
        ethnicity_source_concept_id,care_site_id) VALUES (nextval('person_sequence'),{0},{1},'{2}',0,0,0,0,0,{3})
        RETURNING person_id;
    """.format(gender, year_of_birth, death_datetime if death_datetime else 'NULL', cohort_id if cohort_id else 'NULL')

def build_observation(person_id, field, value='NULL', value_as_concept=0, source_value='NULL',
    date='19700101 00:00:00', visit_id=0):
    """ Build the sql statement for an observation.
    """
    unit_concept_id = field[UNIT_CONCEPT_ID] if field[UNIT_CONCEPT_ID] else 0
    return """INSERT INTO OBSERVATION (observation_id,person_id,observation_concept_id,observation_datetime,
        observation_type_concept_id,value_as_string,value_as_concept_id,visit_occurrence_id,unit_concept_id,
        observation_source_value,observation_source_concept_id,obs_event_field_concept_id) VALUES 
        (nextval('observation_sequence'),{0},{1},'{2}', 32879, '{3}',{4},{5},{6},{7}, 0, 0);
    """.format(person_id, field[CONCEPT_ID], date, value, value_as_concept, visit_id, unit_concept_id, source_value)

def build_measurement(person_id, field, value='NULL', value_as_concept=0, source_value='NULL',
    date='19700101 00:00:00', visit_id=0):
    """ Build the sql statement for a measurement.
    """
    unit_concept_id = field[UNIT_CONCEPT_ID] if field[UNIT_CONCEPT_ID] else 0
    return """INSERT INTO MEASUREMENT (measurement_id,person_id,measurement_concept_id,measurement_datetime,
        measurement_type_concept_id,value_as_number,value_as_concept_id,visit_occurrence_id,unit_concept_id,
        measurement_source_concept_id,value_source_value)
        VALUES (nextval('measurement_sequence'),{0},{1},'{2}',0,{3},{4},{5},{6},0,{7})
    """.format(person_id, field[CONCEPT_ID], date, value, value_as_concept, visit_id, unit_concept_id, source_value)

def build_condition(person_id, field, value='NULL', value_as_concept=0, source_value='NULL', date='19700101 00:00:00', visit_id=0):
    """ Build the sql statement for a condition.
    """
    return """INSERT INTO CONDITION_OCCURRENCE (condition_occurrence_id,person_id,condition_concept_id,
        condition_start_datetime,condition_type_concept_id,condition_status_concept_id,visit_occurrence_id,
        condition_source_value,condition_source_concept_id) VALUES (nextval('condition_sequence'),{0},{1},'{2}',0,0,{3},{4},0)
    """.format(person_id, field[CONCEPT_ID], date, visit_id, source_value)

def build_cohort(cohort_name, location_id):
    """ Build the sql statement for a care site.
    """
    return """
        WITH CS AS (INSERT INTO CARE_SITE (care_site_id,care_site_name,place_of_service_concept_id,
            location_id,care_site_source_value,place_of_service_source_value) SELECT nextval('care_site_sequence'),
            '{0}',0,{1},'{0}','NULL' WHERE NOT EXISTS (SELECT * FROM CARE_SITE WHERE care_site_name='{0}')
            RETURNING care_site_id)
        SELECT care_site_id FROM CS
        UNION
        SELECT care_site_id FROM CARE_SITE WHERE care_site_name='{0}' LIMIT 1
    """.format(cohort_name, location_id)

def build_visit_occurrence(person_id, start_date, end_date):
    """ Build the sql statement for a visit occurence.
    """
    return """INSERT INTO VISIT_OCCURRENCE (visit_occurrence_id,person_id,visit_concept_id,visit_start_date,
        visit_start_datetime,visit_end_date,visit_end_datetime,visit_type_concept_id,visit_source_concept_id,
        admitted_from_concept_id,discharge_to_concept_id) VALUES
        (nextval('visit_occurrence_sequence'), {0}, 0, '{1}', '{1}', '{2}', '{2}', 0, 0, 0, 0)
        RETURNING visit_occurrence_id
    """.format(person_id, start_date, end_date)

def build_location(address):
    """ Build the sql statement to insert a location.
    """
    return """INSERT INTO LOCATION (location_id,address_1) VALUES (nextval('{0}'),'{1}')
    RETURNING location_id""".format(LOCATION_SEQUENCE, address)

def insert_cohort(cohort_name, location_id, pg):
    """ Insert the cohort information.
    """
    return pg.run_sql(build_cohort(cohort_name, location_id), returning=True)

def insert_visit_occurrence(person_id, start_date, end_date, pg):
    """ Insert the visit occurence information.
    """
    return pg.run_sql(build_visit_occurrence(person_id, start_date, end_date), returning=True)

def insert_location(address, pg):
    """ Insert a location.
    """
    return pg.run_sql(build_location(address), returning=True)
