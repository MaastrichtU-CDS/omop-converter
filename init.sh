#!/bin/bash

# SCRIPTS_URL_PREFIX="https://raw.githubusercontent.com/OHDSI/CommonDataModel/v6.0_fixes/PostgreSQL"

# echo "Downloading the OMOP CDM scripts"
# mkdir omop_scripts omop_scripts/v6
# curl -o omop_scripts/v6/OMOP_CDM_postgresql_ddl.txt ${SCRIPTS_URL_PREFIX}/OMOP%20CDM%20postgresql%20ddl.txt
# curl -o omop_scripts/v6/OMOP_CDM_postgresql_constraints.txt ${SCRIPTS_URL_PREFIX}/OMOP%20CDM%20postgresql%20constraints.txt
# curl -o omop_scripts/v6/OMOP_CDM_Results_postgresql_ddl.txt ${SCRIPTS_URL_PREFIX}/OMOP%20CDM%20Results%20postgresql%20ddl.txt
# curl -o omop_scripts/v6/OMOP_CDM_postgresql_pk_indexes.txt ${SCRIPTS_URL_PREFIX}/OMOP%20CDM%20postgresql%20pk%20indexes.txt

# Extract the OMOP CDM scripts with the SQL commands to build the database
unzip postgresql.zip
rm postgresql.zip

# Extract the vocabularies
unzip vocabularies.zip
rm vocabularies.zip

cd cdm_parser
bash
