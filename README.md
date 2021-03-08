# Data Harmonization

Tools used for the data harmonization.

## OMOP Parser CLI

The OMOP parser CLI allows to set up a new Postgres database based on the OMOP CDM specification.

Currently supported database sources:
- csv

### Installation

#### Using docker

Use docker compose `docker-compose run memorabel-parser` to:
- create a container for the postgres database
- start the container with the data harmonization tools

#### Local development

Install the necessary packages following the requirement file.
Make sure that the PATH environment variable includes the path to the Postgres binary.
Otherwise, it'll cause an error when trying to install `psycopg2`.

### Usage

To see more information on the available commands:

```bash
python3 omop_parser_cli.py --help
```

Start by setting up the database configurations:

```bash
python3 omop_parser_cli.py set-up
```
