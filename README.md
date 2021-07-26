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

### Notes

**Representing negative information on a diagnosis**

The OMOP CDM Condition table represents a diagnosis/sign/symptom that suggests the presence of a disease. However, the current model (v6.0) does not provide a clear solution on how to store a negative diagnosis.
One approach that can be followed is to use the Observation table to store this information. A combination of a concept representing the negative conotation (e.g. 'Absence of') and the concept representing the condition can clarify such a case.

This can be done in the following way:
- Create two entries in the destination mapping (one for the Condition table and another for the Observation) and use '_' to ignore the value that shouldn't be added:
```
hypertension,316866,Condition,,,yes/no,4188539/_
no_hypertension,316866,Observation,,,yes/no,_/4132135
```

- Create one entry for each of the variables in the source mapping:
```
hypertension,h_t,0/1,no/yes
no_hypertension,h_t,0/1,no/yes
```

**Inserting the constraints**

To make the process faster, when parsing the data to the OMOP CDM it's recommended to:
1. Create the database and parse the data to the OMOP CDM;
2. Insert the constraints;

**Exporting the database**

There may happend that you'll perform the data parsing in one environment (e.g. locally) but intend to have the database running in another location. For such a case it's recommended to:
1. Create the database without the vocabularies;
2. Parse the data to the OMOP CDM;
3. Export the database;
4. (Optional) Insert the vocabularies;
5. (Optional) Insert the constraints;
6. (Optional) Evaluate the data harmonisation process - QA;
7. In the final location, create the database with the vocabularies;
8. Import the data from the file created at 3;

This will facilitate the process of exporting and transfering by generating a much smaller file.
