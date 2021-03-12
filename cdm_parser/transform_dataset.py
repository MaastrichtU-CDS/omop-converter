import csv
import pandas as pd
from cdm_sql_builder import get_observation, get_condition, get_measurement, get_person
from constants import *

CDM_SQL = {
    CONDITION: get_condition,
    MEASUREMENT: get_measurement,
    OBSERVATION: get_observation
}

def transform_dataset(path, source_mapping, mds_mapping, pg):
    """ Transform the dataset to the CDM format.
    """
    if 'csv' in path:
        with open('../examples/dataset.csv') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for row in csv_reader:
                transform_row(row, source_mapping, mds_mapping, pg)
    elif 'sav' in path:
        df = pd.read_spss(path)
        for index, row in df.iterrows():
            transform_row(row, source_mapping, mds_mapping, pg)

def transform_row(row, source_mapping, mds_mapping, pg):
    """ Transform each row and insert in the database.
    """
    sex_source_variable = source_mapping[SEX][SOURCE_VARIABLE]
    birth_year_source_variable = source_mapping[YEAR_OF_BIRTH][SOURCE_VARIABLE]

    # TODO: Maybe a temporary table for the mapping between person_id and source_person_id
    person_sql = get_person(row[sex_source_variable], row[birth_year_source_variable])
    person_id = pg.run_sql(person_sql, returning=True)

    for key, value in source_mapping.items():
        if mds_mapping[key][DOMAIN] in CDM_SQL:
            statement = CDM_SQL[mds_mapping[key][DOMAIN]](row[value[SOURCE_VARIABLE]], person_id, mds_mapping[key])
            pg.run_sql(statement)
