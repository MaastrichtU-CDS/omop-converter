DB_CONFIGURATION_PATH = '../database.ini'
DB_CONFIGURATION_SECTION = 'postgresql'

OMOP_CDM_DDL_PATH = '../omop_scripts/v6/OMOP_CDM_postgresql_ddl.txt'
OMOP_CDM_PK_PATH = '../omop_scripts/v6/OMOP_CDM_postgresql_pk_indexes.txt'
OMOP_CDM_CONSTRAINTS_PATH = '../omop_scripts/v6/OMOP_CDM_postgresql_constraints.txt'

VOCABULARY_DEFAULT_PATH = '../vocabularies'
DB_DEFAULT_NAME = 'CDM'
DESTINATION_MAPPING_DEFAULT_PATH = '../mappings/destination_mapping.csv'
SOURCE_MAPPING_DEFAULT_PATH = '../mappings/source_mapping.csv'

DB_USER = 'DB_USER'
DB_PASSWORD = 'DB_PASSWORD'
DB_HOST = 'DB_HOST'
DB_PORT = 'DB_PORT'
DB_DATABASE = 'DB_DATABASE'
DOCKER_ENV = 'DOCKER_ENV'
VOCABULARY_PATH = 'VOCABULARY_PATH'
DESTINATION_MAPPING_PATH = 'DESTINATION_MAPPING_PATH'
SOURCE_MAPPING_PATH = 'SOURCE_MAPPING_PATH'
DATASET_PATH = 'DATASET_PATH'

VARIABLE = 'variable'

ID_TABLE = 'person_source_id'

PERSON_SEQUENCE = 'person_sequence'
OBSERVATION_SEQUENCE = 'observation_sequence'
MEASUREMENT_SEQUENCE = 'measurement_sequence'
CONDITION_SEQUENCE = 'condition_sequence'
CARE_SITE_SEQUENCE = 'care_site_sequence'
VISIT_OCCURRENCE = 'visit_occurrence_sequence'
LOCATION_SEQUENCE = 'location_sequence'

CONCEPT_ID = 'concept_id'
SOURCE_VARIABLE = 'source_variable'
DOMAIN = 'domain'
VALUES_MAPPING = 'values_mapping'
VALUES = 'values'
VALUES_PARSED = 'values_parsed'
VALUES_CONCEPT_ID = 'values_concept_id'
UNIT_CONCEPT_ID = 'unit_concept_id'
FORMAT = 'format'
ADDITIONAL_INFO = 'additional_info'
STATIC_VALUE = 'static_value'
ALTERNATIVES = 'alternatives'
CONDITION = 'condition'

VALUE_AS_CONCEPT_ID = 'value_as_concept_id'
DEFAULT_VALUE = '-'
DEFAULT_SEPARATOR = '/'

OBSERVATION = 'Observation'
MEASUREMENT = 'Measurement'
PERSON = 'Person'
CONDITION_OCCURRENCE = 'Condition'
NOT_APPLICABLE = 'NA'

GENDER = 'sex'
YEAR_OF_BIRTH = 'birth_year'
AGE_PREFIX = 'age'
DATE = 'date'
SOURCE_ID = 'source_id'
DEATH_DATE = 'mort_date'
DEATH_FLAG = 'mort'

DATE_FORMAT = '%Y%m%d %H:%M:%S'
DATE_DEFAULT = '19700101 00:00:00'

VOCABULARY_FILES = {
    'DRUG_STRENGTH': 'DRUG_STRENGTH.csv',
    'CONCEPT': 'CONCEPT.csv',
    'CONCEPT_RELATIONSHIP': 'CONCEPT_RELATIONSHIP.csv',
    'CONCEPT_SYNONYM': 'CONCEPT_SYNONYM.csv',
    'VOCABULARY': 'VOCABULARY.csv',
    'RELATIONSHIP': 'RELATIONSHIP.csv',
    'CONCEPT_CLASS': 'CONCEPT_CLASS.csv',
    'DOMAIN': 'DOMAIN.csv'
}
