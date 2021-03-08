DB_CONFIGURATION_PATH = '../database.ini'
DB_CONFIGURATION_SECTION = 'postgresql'

OMOP_CDM_DDL_PATH = '../omop_scripts/v6/OMOP_CDM_postgresql_ddl.txt'
OMOP_CDM_PK_PATH = '../omop_scripts/v6/OMOP_CDM_postgresql_pk_indexes.txt'
OMOP_CDM_CONSTRAINTS_PATH = '../omop_scripts/v6/OMOP_CDM_postgresql_constraints.txt'

DB_USER = 'DB_USER'
DB_PASSWORD = 'DB_PASSWORD'
DB_HOST = 'DB_HOST'
DB_PORT = 'DB_PORT'
DB_DATABASE = 'DB_DATABASE'
DOCKER_ENV = 'DOCKER_ENV'
VOCABULARY_PATH = 'VOCABULARY_PATH'

VARIABLE = 'variable'

PERSON_SEQUENCE = 'person_sequence'
OBSERVATION_SEQUENCE = 'observation_sequence'
MEASUREMENT_SEQUENCE = 'measurement_sequence'
CONDITION_SEQUENCE = 'condition_sequence'

CONCEPT = 'concept'
SOURCE_VARIABLE = 'source_variable'
DOMAIN = 'domain'
OBSERVATION = 'observation'
MEASUREMENT = 'measurement'
CONDITION = 'condition'
SEX = 'sex'
YEAR_OF_BIRTH = 'year_of_birth'

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
