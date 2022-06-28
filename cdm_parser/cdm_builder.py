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

def drop_database():
    """ Drop the CDM database.
    """
    print(f'Dropping the database {os.environ[DB_DATABASE]}')
    with PostgresManager(default_db=True, isolation_level=ISOLATION_LEVEL_AUTOCOMMIT) as pg:
        pg.run_sql(f'DROP DATABASE "{os.environ[DB_DATABASE]}";')

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

def create_sequences(pg, sequence_start=1):
    """ Create the sequences needed.
    """
    print('Create sequences')
    for sequence in [PERSON_SEQUENCE, OBSERVATION_SEQUENCE, MEASUREMENT_SEQUENCE,
        CONDITION_SEQUENCE, CARE_SITE_SEQUENCE, VISIT_OCCURRENCE, LOCATION_SEQUENCE]:
        pg.create_sequence(sequence, start=sequence_start)

def set_cdm_source(pg, cdm_release_date):
    """ Set the necessary information in the CDM Source table.
    """
    pg.run_sql(
        """INSERT INTO CDM_SOURCE (cdm_source_name,cdm_holder,source_description,source_documentation_reference,
            cdm_etl_reference,cdm_release_date,cdm_version,vocabulary_version) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
        parameters=(os.getenv(SOURCE_NAME), os.getenv(HOLDER), os.getenv(DESCRIPTION), os.getenv(REFERENCE),
        os.getenv(ETL_REFERENCE), cdm_release_date, os.getenv(CDM_VERSION), os.getenv(VOCABULARY_VERSION))
    )

def create_id_table(pg):
    """ Create the table to store the link between person id and the source id.
    """
    print(f'Create id table: {ID_TABLE}')
    pg.run_sql(f'CREATE TABLE IF NOT EXISTS {ID_TABLE} \
        (person_id bigint PRIMARY KEY, source_id varchar(100), cohort_id varchar(100) NOT NULL)')

def get_person_id(source_id, cohort_id, pg):
    """ Retrieve the person id from the source id.
    """
    return pg.run_sql(
        f"SELECT person_id FROM {ID_TABLE} WHERE source_id='{source_id}' AND cohort_id='{cohort_id}' LIMIT 1",
        fetch_one=True,
    )

def insert_id_record(source_id, person_id, cohort_id, pg):
    """ Insert a new record in the temporary table.
    """
    return pg.run_sql(f"INSERT INTO {ID_TABLE} VALUES ({person_id}, '{source_id}', '{cohort_id}')")

def build_person(gender, year_of_birth, cohort_id, death_datetime):
    """ Build the sql statement for a person.
    """
    return (("""INSERT INTO PERSON (person_id,gender_concept_id,year_of_birth,death_datetime,
        race_concept_id,ethnicity_concept_id,gender_source_concept_id,race_source_concept_id,
        ethnicity_source_concept_id,care_site_id) VALUES (nextval('person_sequence'),%s,%s,%s,0,0,0,0,0,%s)
        RETURNING person_id;
    """), (gender, year_of_birth, death_datetime, cohort_id))

def update_person(person_id, death_datetime):
    """ Build the sql statement to update a person.
    """
    return (("""UPDATE PERSON SET death_datetime = %s WHERE person_id = %s;"""),
        (death_datetime, person_id))

def build_observation(person_id, field, value=None, value_as_concept=None, source_value=None,
    date='19700101 00:00:00', visit_id=None, additional_info=None):
    """ Build the sql statement for an observation.
    """
    unit_concept_id = field[UNIT_CONCEPT_ID] if field[UNIT_CONCEPT_ID] else None
    return (("""INSERT INTO OBSERVATION (observation_id,person_id,observation_concept_id,observation_datetime,
        observation_type_concept_id,value_as_string,value_as_concept_id,visit_occurrence_id,unit_concept_id,
        observation_source_value,observation_source_concept_id,obs_event_field_concept_id) VALUES 
        (nextval('observation_sequence'),%s,%s,%s, 32879, %s,%s,%s,%s,%s, 0, 0);
    """), (person_id, field[CONCEPT_ID], date, value, value_as_concept, visit_id, unit_concept_id, source_value))

def build_measurement(person_id, field, value=None, value_as_concept=None, source_value=None,
    date='19700101 00:00:00', visit_id=None, additional_info=None):
    """ Build the sql statement for a measurement.
    """
    unit_concept_id = field[UNIT_CONCEPT_ID] if field[UNIT_CONCEPT_ID] else None
    return ("""INSERT INTO MEASUREMENT (measurement_id,person_id,measurement_concept_id,measurement_datetime,
        measurement_type_concept_id,value_as_number,value_as_concept_id,visit_occurrence_id,unit_concept_id,
        measurement_source_value,measurement_source_concept_id,value_source_value)
        VALUES (nextval('measurement_sequence'),%s,%s,%s,0,%s,%s,%s,%s,%s,0,%s)
    """, (person_id, field[CONCEPT_ID], date, value, value_as_concept, visit_id, unit_concept_id, additional_info, source_value))

def build_condition(person_id, field, value=None, value_as_concept=None, source_value=None,
    date='19700101 00:00:00', visit_id=None, additional_info=None):
    """ Build the sql statement for a condition.
    """
    return (("""INSERT INTO CONDITION_OCCURRENCE (condition_occurrence_id,person_id,condition_concept_id,
        condition_start_datetime,condition_type_concept_id,condition_status_concept_id,visit_occurrence_id,
        condition_source_value,condition_source_concept_id,condition_status_source_value) VALUES
        (nextval('condition_sequence'),%s,%s,%s,0,0,%s,%s,0,%s)
    """), (person_id, field[CONCEPT_ID], date, visit_id, source_value, additional_info))

def check_duplicated_observation(person_id, field, value=None, value_as_concept=None, source_value=None,
    date='19700101 00:00:00', visit_id=None, additional_info=None):
    return ((f"""SELECT COUNT(observation_id) FROM OBSERVATION WHERE person_id=%s AND observation_concept_id=%s
        AND observation_datetime=%s AND {"value_as_string='%s'" if value else "value_as_concept_id=%s"} AND visit_occurrence_id=%s 
    """), (person_id, field[CONCEPT_ID], date, value or value_as_concept, visit_id))

def check_duplicated_measurement(person_id, field, value=None, value_as_concept=None, source_value=None,
    date='19700101 00:00:00', visit_id=None, additional_info=None):
    return ((f"""SELECT COUNT(measurement_id) FROM MEASUREMENT WHERE person_id=%s AND
        measurement_concept_id=%s AND measurement_datetime=%s AND {"value_as_number=%s" if value
        else "value_as_concept_id=%s"} AND visit_occurrence_id=%s
    """), (person_id, field[CONCEPT_ID], date, value or value_as_concept, visit_id))

def check_duplicated_condition(person_id, field, value=None, value_as_concept=None, source_value=None,
    date='19700101 00:00:00', visit_id=None, additional_info=None):
    return (("""SELECT COUNT(condition_occurrence_id) FROM CONDITION_OCCURRENCE WHERE person_id=%s AND 
        condition_concept_id=%s AND condition_start_datetime=%s AND visit_occurrence_id=%s
    """), (person_id, field[CONCEPT_ID], date, visit_id))

def build_cohort(cohort_name, location_id):
    """ Build the sql statement for a care site.
    """
    return """
        WITH CS AS (INSERT INTO CARE_SITE (care_site_id,care_site_name,place_of_service_concept_id,
            location_id,care_site_source_value,place_of_service_source_value) SELECT nextval('care_site_sequence'),
            '{0}',0,{1},'{0}',NULL WHERE NOT EXISTS (SELECT * FROM CARE_SITE WHERE care_site_name='{0}')
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
    return pg.run_sql(build_cohort(cohort_name, location_id), fetch_one=True)

def insert_visit_occurrence(person_id, start_date, end_date, pg):
    """ Insert the visit occurence information.
    """
    return pg.run_sql(build_visit_occurrence(person_id, start_date, end_date), fetch_one=True)

def insert_location(address, pg):
    """ Insert a location.
    """
    return pg.run_sql(build_location(address), fetch_one=True)

def get_number_of_persons(pg):
    """ Get the total number of persons in the database.
    """
    return pg.run_sql("""SELECT gender_concept_id, COUNT(person_id), 
        MAX(year_of_birth), MIN(year_of_birth), AVG(year_of_birth) FROM 
        PERSON GROUP BY gender_concept_id""", fetch_all=True)

def count_entries(pg):
    """ Count entries for observations, measurements, and conditions.
    """
    entries = {
        'OBSERVATION': 'observation_id',
        'MEASUREMENT': 'measurement_id',
        'CONDITION_OCCURRENCE': 'condition_occurrence_id'
    }
    count = []
    for key, value in entries.items():
        count.append(pg.run_sql(f'SELECT count({value}) FROM {key};', fetch_one=True))
    return count

def get_visit_by_person_and_date(pg, person_id, start_date):
    """ Retrieve the visit id for a person in a specific date.
    """
    return pg.run_sql(f"""SELECT visit_occurrence_id FROM VISIT_OCCURRENCE WHERE 
        person_id = {person_id} AND visit_start_datetime = '{start_date}'""", fetch_one=True)

def get_visit_occurrences(pg):
    """ Get all visit occurences.
    """
    #keys = ['visit_id', 'person_id', 'year_of_birth', 'gender_concept_id', 'death_datetime']
    return pg.run_sql("""SELECT v.visit_occurrence_id, v.visit_start_datetime, p.person_id, p.year_of_birth,
        p.gender_concept_id, p.death_datetime FROM VISIT_OCCURRENCE AS v JOIN PERSON AS p ON 
        p.person_id = v.person_id""", fetch_all=True)

def get_observations_by_visit_id(pg, visit_id):
    """ Get observation by visit id and person id.
    """
    return pg.run_sql(f"""SELECT observation_concept_id, observation_datetime, value_as_string, value_as_concept_id 
        FROM OBSERVATION WHERE visit_occurrence_id = {visit_id};""", fetch_all=True)

def get_measurements_by_visit_id(pg, visit_id):
    """ Get observation by visit id and person id.
    """
    return pg.run_sql(f"""SELECT measurement_concept_id, measurement_datetime, value_as_number 
        FROM MEASUREMENT WHERE visit_occurrence_id = {visit_id};""", fetch_all=True)

def get_conditions_by_visit_id(pg, visit_id):
    """ Get observation by visit id and person id.
    """
    return pg.run_sql(f"""SELECT condition_concept_id, condition_start_datetime 
        FROM CONDITION_OCCURRENCE WHERE visit_occurrence_id = {visit_id};""", fetch_all=True)

def insert_values(pg, table_name, columns):
    """ Insert values into a table according to the specification from the parameters.
    """
    return pg.run_sql(f"""INSERT INTO {table_name} ({', '.join(columns.keys())}) 
        VALUES ({', '.join([f"'{str(val)}'" if str(val) else 'NULL' for val in columns.values()])})""")
