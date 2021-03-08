import click
import os
from postgres_manager import PostgresManager
from utils import export_config, import_config, run_command
from constants import *
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from cdm_sql_builder import get_person, get_observation, get_condition, get_measurement
from parser import parse_csv_mapping
import csv

@click.group()
def cli():
    click.echo('OMOP parser CLI')

@cli.command()
@click.option('--user', prompt=True)
@click.option('--password', prompt=True, hide_input=True)
@click.option('--host', prompt=True)
@click.option('--port', prompt=True)
@click.option('--database-name', prompt=True)
@click.option('--vocabulary-path', prompt=True)
def set_up(user, password, host, port, database_name, vocabulary_path):
    """ Set up the configurations needed.
    """
    configurations = {
        DB_USER: user,
        DB_PASSWORD: password,
        DB_HOST: host,
        DB_PORT: port,
        DB_DATABASE: database_name,
        VOCABULARY_PATH: vocabulary_path
    }
    export_config(DB_CONFIGURATION_PATH, DB_CONFIGURATION_SECTION, configurations)

@cli.command()
def create_db():
    """ Create a new OMOP CDM database.
    """
    if DOCKER_ENV not in os.environ: import_config(DB_CONFIGURATION_PATH, DB_CONFIGURATION_SECTION)
    click.echo(f'Create the database {os.environ[DB_DATABASE]}')
    with PostgresManager(default_db=True, isolation_level=ISOLATION_LEVEL_AUTOCOMMIT) as pg:
            pg.create_database(os.environ[DB_DATABASE])

    with PostgresManager() as pg:
        pg.execute_file(OMOP_CDM_DDL_PATH)
        if VOCABULARY_PATH in os.environ:
            click.echo('Insert the vocabulary')
            for table, vocabulary_file in VOCABULARY_FILES.items():
                click.echo(f'Populating the {table} table')
                pg.copy_from_file(table, f'{os.environ[VOCABULARY_PATH]}/{vocabulary_file}')

@cli.command()
def transform():
    """ Populate the OMOP CDM database.
    """
    if DOCKER_ENV not in os.environ: import_config(DB_CONFIGURATION_PATH, DB_CONFIGURATION_SECTION)
    with PostgresManager() as pg:
        for sequence in [PERSON_SEQUENCE, OBSERVATION_SEQUENCE, MEASUREMENT_SEQUENCE, CONDITION_SEQUENCE]:
            pg.create_sequence(sequence)
        
        mds_mapping = parse_csv_mapping('./mappings/mds_mapping.csv')
        # TODO: The source mapping and dataset (possibly the mds_mapping) should be retrieved from a configuration file.
        # Maybe they can be args for the cli command
        source_mapping = parse_csv_mapping('../examples/source_mapping.csv')
        
        # TODO: create the statements and commit them in batches

        CDM_SQL = {
            CONDITION: get_condition,
            MEASUREMENT: get_measurement,
            OBSERVATION: get_observation
        }

        with open('../examples/dataset.csv') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for row in csv_reader:
                sex_source_variable = source_mapping[SEX][SOURCE_VARIABLE]
                birth_year_source_variable = source_mapping[YEAR_OF_BIRTH][SOURCE_VARIABLE]

                # TODO: Maybe a temporary table for the mapping between person_id and source_person_id
                person_sql = get_person(row[sex_source_variable], row[birth_year_source_variable])
                person_id = pg.run_sql(person_sql, returning=True)

                for key, value in source_mapping.items():
                    if mds_mapping[key][DOMAIN] in CDM_SQL:
                        statement = CDM_SQL[mds_mapping[key][DOMAIN]](row[value[SOURCE_VARIABLE]], person_id, mds_mapping[key])
                        pg.run_sql(statement)

@click.option('--file', default='../omop_cdm_export.pgsql')
@cli.command()
def export_db(file):
    """ Export the database to a file.
    """
    if DOCKER_ENV not in os.environ: import_config(DB_CONFIGURATION_PATH, DB_CONFIGURATION_SECTION)
    process = run_command(['pg_dump', '-U', os.getenv(DB_USER), os.getenv(DB_DATABASE), '-f', file])
    if process.returncode == 0:
        click.echo('Successfully exported the database.')
    else:
        click.echo(f'Failed to export the database: {process.stderr.decode("utf-8")}')

@click.option('--file', default='../omop_cdm_export.pgsql')
@cli.command()
def import_db(file):
    """ Create and build a database from a file.
    """
    if DOCKER_ENV not in os.environ: import_config(DB_CONFIGURATION_PATH, DB_CONFIGURATION_SECTION)
    with PostgresManager(default_db=True, isolation_level=ISOLATION_LEVEL_AUTOCOMMIT) as pg:
        pg.create_database(os.environ[DB_DATABASE])
    process = run_command(['psql', '-U', os.getenv(DB_USER), os.getenv(DB_DATABASE), '-f', file])
    if process.returncode == 0:
        click.echo('Successfully imported the database.')
    else:
        click.echo(f'Failed to import the database: {process.stderr.decode("utf-8")}')

if __name__ == '__main__':
    cli()
