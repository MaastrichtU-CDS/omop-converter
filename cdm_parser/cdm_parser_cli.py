import click
import os
from datetime import datetime

from utils import export_config, import_config, run_command, parse_output
from constants import *
from cdm_builder import *
from parser import parse_csv_mapping
from parse_dataset import DataParser
from postgres_manager import PostgresManager
from parse_mapping import parse_mapping_to_columns, parse_visit

@click.group()
def cli():
    click.echo('OMOP parser CLI')
    if DOCKER_ENV not in os.environ: import_config(DB_CONFIGURATION_PATH, DB_CONFIGURATION_SECTION)

@cli.command(help='Set up the configurations when using the CLI without docker.')
@click.option('--user', prompt=True)
@click.option('--password', prompt=True, hide_input=True)
@click.option('--host', prompt=True)
@click.option('--port', prompt=True)
@click.option('--database-name', prompt=True, default=DB_DEFAULT_NAME)
@click.option('--vocabulary-path', prompt=True, default=VOCABULARY_DEFAULT_PATH)
@click.option('--destination-mapping', prompt=True, default=DESTINATION_MAPPING_DEFAULT_PATH)
@click.option('--source-mapping', prompt=True, default=SOURCE_MAPPING_DEFAULT_PATH)
@click.option('--dataset', prompt=True)
@click.option('--dataset-delimiter', prompt=False, default=DEFAULT_DELIMITER)
@click.option('--follow-up-suffix', prompt=False, default=None)
@click.option('--encoding', prompt=False, default=None)
@click.option('--missing-values', prompt=False, default=None)
@click.option('--ignore-duplicates', prompt=False, default=False)
def set_up(user, password, host, port, database_name, vocabulary_path, destination_mapping,
    source_mapping, dataset, dataset_delimiter, follow_up_suffix, encoding, missing_values,
    ignore_duplicates):
    """ Set up the configurations needed.
    """
    configurations = {
        DB_USER: user,
        DB_PASSWORD: password,
        DB_HOST: host,
        DB_PORT: port,
        DB_DATABASE: database_name,
        VOCABULARY_PATH: vocabulary_path,
        DESTINATION_MAPPING_PATH: destination_mapping,
        SOURCE_MAPPING_PATH: source_mapping,
        DATASET_PATH: dataset,
        DATASET_DELIMITER: dataset_delimiter,
        FOLLOW_UP_SUFFIX: follow_up_suffix,
        ENCODING: encoding,
        MISSING_VALUES: missing_values,
        IGNORE_DUPLICATES: ignore_duplicates,
    }
    export_config(DB_CONFIGURATION_PATH, DB_CONFIGURATION_SECTION, configurations)

@cli.command(help='Set up the CDM database')
@click.option(
    '--insert-voc/--no-insert-voc',
    default=False,
    type=bool,
    help='Insert the vocabulary?',
)
@click.option(
    '--sequence-start',
    default=1,
    type=int,
    help='Start value for the sequences that will be created (default 1)'
)
@click.option(
    '--create-db/--no-create-db',
    default=True,
    type=bool,
    help='Create the database? (default: True)',
)
def set_db(insert_voc, sequence_start, create_db):
    """ Set up the CDM database:
        * Create new database with the CDM schema;
        * Create the sequences used to obtain de id's (a start value
        can be provided, may be useful in a case where data from multiple
        data sources will be parsed and used in the same database)
        * Optionally: Insert the vocabulary if available;
    """
    if create_db:
        create_database()
    with PostgresManager() as pg:
        set_schema(pg)
        if os.getenv(SOURCE_NAME):
            set_cdm_source(pg, datetime.today().strftime('%Y-%m-%d'))
        create_sequences(pg, sequence_start)
        if insert_voc and VOCABULARY_PATH in os.environ:
            insert_vocabulary(pg)

@click.option(
    '--truncate/--no-truncate',
    default=False,
    type=bool,
    help='Truncate the vocabulary tables'
)
@cli.command()
def insert_voc(truncate):
    """ Insert the vocabularies.
    """
    with PostgresManager() as pg:
        if truncate:
            truncate_vocabulary(pg)
        if VOCABULARY_PATH in os.environ:
            insert_vocabulary(pg)

@cli.command()
def drop_db():
    """ Drop the CDM database.
    """
    drop_database()

@cli.command()
def insert_constraints():
    """ Set the CDM primary keys and constraints.
    """
    with PostgresManager() as pg:
        set_constraints(pg)

@cli.command(help='Parse the dataset and populate the OMOP CDM database.')
@click.option('--cohort-name', prompt=True)
@click.option('--cohort-location')
@click.option('--start', default=0, type=int)
@click.option('--limit', default=-1, type=int)
@click.option(
    '--convert-categoricals/--no-convert-categoricals',
    default=False,
    type=bool,
    help='Convert the caregories? Only valid for spss files'
)
@click.option('--drop-temp-tables/--no-drop-temp-tables', default=False, type=bool)
def parse_data(cohort_name, cohort_location, start, limit, convert_categoricals, drop_temp_tables):
    """ Parse the source dataset and populate the CDM database.
        
        Important: One or more temporary tables will be created to store information only required
        during this process. By default, these tables will be dropped by the end. There is an
        option to avoid dropping these tables that may be useful in cases with multiple files.
        Nonetheless, in any case, the tables should not exist after parsing all the data and 
        it's recommended to manually check if all tables with prefix 'temp' were removed successfully.
    """
    destination_mapping = parse_csv_mapping(os.getenv(DESTINATION_MAPPING_PATH))
    source_mapping = parse_csv_mapping(os.getenv(SOURCE_MAPPING_PATH))

    # TODO: create the statements and commit them in batches
    with PostgresManager() as pg:
        # Insert the cohort information
        # If the cohort is already in the DB it'll only retrieve the id
        cohort_id = None
        if cohort_name:
            location_id = insert_location(cohort_location if cohort_location else cohort_name, pg)
            cohort_id = insert_cohort(cohort_name, location_id, pg)
            print(f"Cohort id {str(cohort_id)}")

        # Create the necessary temporary table
        create_id_table(pg)

        # Parse the dataset
        parser = DataParser(
            source_mapping,
            destination_mapping,
            os.getenv(FOLLOW_UP_SUFFIX),
            os.getenv(FOLLOW_UP_PREFIX),
            cohort_id,
            os.getenv(MISSING_VALUES),
            os.getenv(IGNORE_DUPLICATES),
            pg
        )
        DataParser.parse_dataset(
            os.getenv(DATASET_PATH),
            start,
            limit,
            convert_categoricals, 
            delimiter=os.getenv(DATASET_DELIMITER) or DEFAULT_DELIMITER,
            bulk=os.getenv(BULK),
            bulk_range=os.getenv(BULK_RANGE) or 50,
            callback=parser.transform_rows,
        )

        # Dropping the temporary tables
        if drop_temp_tables:
           pg.drop_table(ID_TABLE)

@click.option('--table-name', prompt=True)
@click.option('--cohort-id', default=None, type=int)
@click.option('--drop-table', default=1, type=int)
@cli.command()
def parse_omop_to_plane(table_name, cohort_id, drop_table):
    """ Parse the OMOP content to a plane/simpified table. Available to 
        facilitate the first contact with SQL databases and querying. However,
        it's recommended to use the OMOP table (and develop any scripts or algorithms 
        for the OMOP schema) since it represents the primary source of data and
        a standard clinical model.
    """
    destination_mapping = parse_csv_mapping(os.getenv(DESTINATION_MAPPING_PATH))
    with PostgresManager() as pg:
        if drop_table:
            print("Drop table")
            pg.drop_table(table_name)
        else:
            print("Delete cohort rows")
            delete_by_cohort(pg, table_name, cohort_id)
        # Transform the mapping variables into columns and create the table
        columns = parse_mapping_to_columns(destination_mapping)
        pg.create_table(table_name, columns.values())
        print(f'Table {table_name} created successfully')
        # Parse the data from OMOP to the simplified table
        print('Parsing the OMOP CDM data to the plane table')
        parsed_visits = []
        visits = get_visit_occurrences(pg, cohort_id)
        for count, visit in enumerate(visits):
            # Retrieve the observations, measurements, and conditions for each
            # visit (visit[0] - the visit ID)
            observations = get_observations_by_visit_id(pg, visit[0])
            measurements = get_measurements_by_visit_id(pg, visit[0])
            conditions = get_conditions_by_visit_id(pg, visit[0])
            visit_values = parse_visit(
                destination_mapping, columns, visit, observations, measurements, conditions)
            if os.getenv(BULK):
                parsed_visits.append(visit_values)
                bulk_range = int(os.getenv(BULK_RANGE)) or 50
                if len(parsed_visits) == bulk_range or count == len(visits) - 1:
                    insert_values(pg, table_name, parsed_visits)
                    print(f"Bulk insert: {count + 1} rows")
                    parsed_visits = []
            else:
                insert_values(pg, table_name, [visit_values])
            if (count + 1) % 1000 == 0:
                print(f'Processed {count + 1} visits from {len(visits)}')

@click.option(
    '--convert-categoricals/--no-convert-categoricals',
    default=False,
    type=bool,
    help='Convert the caregories? Only valid for spss files'
)
@cli.command()
def info(convert_categoricals):
    """ Returns information regarding the mapping and dataset:
            - Errors in the source mapping.
            - Variables missing in the destination mapping.
            - Variables available in the dataset that weren't included.
    """
    destination_mapping = parse_csv_mapping(os.getenv(DESTINATION_MAPPING_PATH))
    source_mapping = parse_csv_mapping(os.getenv(SOURCE_MAPPING_PATH))

    parser = DataParser(
        source_mapping,
        destination_mapping,
        os.getenv(FOLLOW_UP_SUFFIX),
        os.getenv(FOLLOW_UP_PREFIX),
        None,
        os.getenv(MISSING_VALUES),
        os.getenv(IGNORE_DUPLICATES),
        None
    )

    header = DataParser.parse_dataset(
        os.getenv(DATASET_PATH),
        0,
        convert_categoricals, 
        delimiter=os.getenv(DATASET_DELIMITER) or DEFAULT_DELIMITER,
        convert_categoricals=convert_categoricals,
        callback=lambda *args, **kwargs: None,
    )

    info = {
        SOURCE_MAPPING: {
            ERROR_MESSAGE: 'Source mapping incomplete: ',
            MESSAGE: 'Source mapping complete.',
            VARIABLES: []
        },
        DESTINATION_MAPPING: {
            ERROR_MESSAGE: 'Destination mapping incomplete: ',
            MESSAGE: 'Destination mapping complete.',
            VARIABLES: []
        },
        DATASET: {
            ERROR_MESSAGE: 'Variables available in the dataset and not included: ',
            MESSAGE: 'All variables from the dataset included in the mapping.',
            VARIABLES: [],
        },
        VARIABLES: {
            ERROR_MESSAGE: 'Variables on the source mapping not available in the dataset: ',
            MESSAGE: 'All variables from the source mapping found in the dataset.',
            VARIABLES: [],
        }
    }
    source_variables = []
    for key, value in source_mapping.items():
        #source_variables.append(value[SOURCE_VARIABLE])
        source_variable = value[SOURCE_VARIABLE]
        source_variables.append([source_variable] + \
                    [prefix + source_variable for prefix in parser.fu_prefix] + \
                        [source_variable + suffix for suffix in parser.fu_suffix])
        if value[VALUES]:
            if not value[VALUES_PARSED]:
                info[SOURCE_MAPPING][VARIABLES].append(key)
            if key not in destination_mapping or VALUES_CONCEPT_ID not in destination_mapping[key]:
                info[DESTINATION_MAPPING][VARIABLES].append(key)
        if value[SOURCE_VARIABLE] and value[SOURCE_VARIABLE] not in header:
            info[VARIABLES][VARIABLES].append(key)

    # Columns available in the dataset and unused
    info[DATASET][VARIABLES] = [column for column in header if column not in source_variables]

    for value in info.values():
        if len(value[VARIABLES]) > 0:
            print(parse_output(value[ERROR_MESSAGE] + ', '.join(value[VARIABLES])))
        else:
            print(parse_output(value[MESSAGE]))

@cli.command()
def report():
    """ Returns information that can be use for quality control.
    """
    with PostgresManager() as pg:
        persons = get_number_of_persons(pg)
        if persons:
            for persons_by_gender in persons:
                print(f'Gender: {persons_by_gender[0]} - Count: {persons_by_gender[1]}; '
                    + f'Max/Min/Avg Year of Birth: {persons_by_gender[2]}/{persons_by_gender[3]}/{round(persons_by_gender[4], 2)};')
        entry_count = count_entries(pg)
        print(f'{entry_count[0]} observations, {entry_count[1]} measurements, {entry_count[2]} conditions')

@click.option('-f', '--file', default='/mnt/data/omop_cdm_export.pgsql',
    help='Path for the output file')
@click.option('--data-only/--no-data-only', default=False, help='Export only data and not the DDL', type=bool)
@cli.command()
def export_db(file, data_only):
    """ Export the database to a file.
    """
    command = ['pg_dump', '-d', PostgresManager.get_database_uri(), '-f', file]
    if data_only:
        command.append('--data-only')
    run_command(
        command,
        'Successfully exported the database.',
        'Failed to export the database.')

@click.option('-f', '--file', default='/mnt/data/omop_cdm_export.pgsql',
    help='Path for the file to import')
@click.option('--create-db/--no-create-db', default=False, help='Create the database?', type=bool)
@cli.command()
def import_db(file, create_db):
    """ Create and build a database from a file.
    """
    if create_db:
        create_database()
    run_command(
        ['psql', '-d', PostgresManager.get_database_uri(), '-f', file],
        'Successfully imported the database.',
        'Failed to import the database.')

if __name__ == '__main__':
    cli()
