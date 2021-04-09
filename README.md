# Data Harmonization

Tools used for the data harmonization.

## OMOP Parser CLI

The OMOP parser CLI allows to set up a new Postgres database based on the OMOP CDM specification.

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
