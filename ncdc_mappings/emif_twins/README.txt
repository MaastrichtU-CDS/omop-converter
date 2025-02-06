# EMIF-EPAD node documentation - steps to harmonize the dataset

Notes:
- in the baseline datasets + cognition, the participant ID variable is named: "Record ID" (visit date: DATA_NEURO)
- in the Twins_export_FU3.csv, the participant ID variable is named: "Participant Id" (visit date: DATE_SCR)

In addition, the Twins_export_FU3.csv seems to have some formatting problems and a lot of missing data.
Due to this, a specific source_mapping file should be used (only a few variables are harmonized).

Steps to harmonize the datasets:
1) Run the docker compose file
2) Create the database
     python3 cdm_parser_cli.py set-db
3) Harmonize the baseline and cognitive files
     export DATASET_PATH=/mnt/data/file_name.csv
     export SOURCE_MAPPING_PATH=/mnt/data/source_mapping_record_id.csv
4) Harmonize the Twins_export_FU3.csv file
     export DATASET_PATH=/mnt/data/Twins_export_FU3.csv
     export SOURCE_MAPPING_PATH=/mnt/data/source_mapping.csv
5) Create the ncdc table
     python3 cdm_parse_cli.py parse-omop-to-plane


The docker container is specific for this cohort: pmateus/memorabel-data-harmonization-twins:1.1.20
This was necessary since the csv files has errors and the pandas reader worked better.
In the parse_dataset.py file:
'''
   if '.csv' in path:
       df = pd.read_csv(path, encoding=os.getenv(ENCODING), on_bad_lines='skip', delimiter=delimiter)
       callback(df.loc[start:].iterrows(), **kwargs)
'''