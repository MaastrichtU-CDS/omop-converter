# Data Harmonization

Tools used for the data harmonization.

## CDM Parser CLI

The CDM parser CLI provides a way to harmonize the data from different sites to a common data model. It allows to set up and populate a new Postgres database based on the OMOP (Observational Medical Outcomes Partnership) CDM (Common Data Model).

Steps:
- Evaluate the variables used in the project, define global names and map them to concepts (either from the Athena vocabularies or your own)
- Create a mapping between the variables and the concepts (e.g. `./examples/destination_mapping.csv`)
- For each site with a different database schema/codebook, create a mapping between the source variables and the global variables defined previously (e.g. `./examples/source_mapping.csv`)
- Correctly configure the path to the mappings and dataset
- Run the CDM Parser to build and populate the postgres database 

Currently supported database sources:
- csv
- SPSS data files (sav)
- sas

### Installation

#### Using docker

Use the `docker-compose.yaml` file to make any change needed e.g. use different paths for the files needed.
The necessary file structure should be:
- The source mapping at `$PWD/mappings/source_mapping.csv`
- The destination mapping at `$PWD/mappings/destination_mapping.csv`
- The dataset at `$PWD/data/dataset.csv`

Use docker compose `docker-compose run memorabel-parser` to:
- create a container for the postgres database and a manager (`localhost:8080`)
- start the container with the data harmonization tools

To stop the containers and remove the different components use `docker-compose down`.

#### Local development

Install the necessary packages following the requirement file.
Make sure that the PATH environment variable includes the path to the Postgres binary.
Otherwise, it'll cause an error when trying to install `psycopg2`.

### Usage

To see more information on the available commands:

```bash
python3 cdm_parser_cli.py --help
```

(Local Development) Start by setting up the database configurations:

```bash
python3 cdm_parser_cli.py set-up
```
