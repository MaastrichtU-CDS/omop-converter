# Data Harmonization

Tools used for the data harmonization in the NCDC project.

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

### Mappings

As previously described, there is the need to develop mappings that will establish the source variable that needs to be harmonized and in what way this should be done.

**Destination Mapping**

The destination mapping represents each variable that should be harmonized and optionally the standard used to represent it.

The parameters used to do this representation are:
- variable: identifier of the variable (the same that will be used in the `source mapping`);
- concept_id: the concept id that will represent this variable in the OMOP CDM;
- domain: the domain (equivalent to the table where it'll be inserted) to which this variable belongs;
- unit: the units for the variable (if applicable);
- unit_concept_id: the concept id that represents this unit;
- values: used to map categorical values (`;` used to separate each value) to the standardized equivalent;
- values_concept_id: the concept id (standardized equivalent) of each value;
- additional_info: static information or another variable that provides additional information;
- date: specific date representing the time when the value was obtained;
- values_range: the range of values that will be used in the plane table;
- type: the type of the variable;
  
```
variable,concept_id,domain,unit,unit_concept_id,values,values_concept_id,additional_info,date,values_range,type
education_category,4171617,Observation,,,low;high,4267416;4328749,,,0;1,int
education_category_verhage,2000000078,Observation,,,1;2;3;4;5;6;7,,,,1;2;3;4;5;6;7,int
```

In the prior example:
- the variable `education_category` will be harmonized to use the standard concept id `4171617`. The value `low` will be represented using the concept id `4267416` and the value `high` the concept id `4328749`. In the plane table, these values will be represented with `0` and `1` and the type will be `integer`
- the variable `education_category_verhage` will be harmonized to a new concept with id `2000000078` that doesn't belong to one of the OHDSY vocabularies and was created for this project;

**Source Mapping**

The source mapping connects each variable that will be harmonized with the corresponding name in the source database.

The parameters used to do this representation are:
- variable: identifier of the variable;
- source_variable: variable name in the source dataset;
- alternatives: variable names from the source dataset that can be used as alternatives if the first one is empty;
- condition: optional condition that represents one or more values that will be compared with the values from the source data;
- values: used to map categorical values (`;` used to separate each value) to the values identifiers from the destination mapping;
- values_parsed: values identifiers from the destination mapping;
- format: format for the value (currently only used to parse dates);
- static_value: static data that may be included;

```
variable,source_variable,alternatives,condition,values,values_parsed,format,static_value
birth_year,year_of_birth,,,,,,
date_age,visit1,,,,,%Y-%m-%d,
sex,sex,,,1.0;2.0;-,male;female;-,,
education_category_3,education_cat,,,3.0;2.0;1.0,low;medium;high,,
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

The data harmonisation process can be executed in a different number of ways depending on the resources available. It can be performed in a local environment and later transferred to the final location (recommended since it allows more control to validate and check potential problems) or directly done in the final location. For the first case, there is also the possibility of creating a "light weight database" (without the vocabularies) so that the exported file can be more easily transferred (including the vocabularies may represent an overhead of around 1GB). Additionally, there is also the possibility to export only the data (avoid the statements that create the CDM schema), this may be useful when inserting data from multiple sources in the same database.

In a case where you'll perform the data parsing in one environment (e.g. locally) but intend to have the database running in another location, it's recommended to:
1. Create the database without the vocabularies;
2. Parse the data to the OMOP CDM;
3. Export the database;
4. (Optional) Insert the vocabularies;
5. (Optional) Insert the constraints;
6. (Optional) Evaluate the data harmonisation process - QA;
7. In the final location, create the database with the vocabularies;
8. Import the data from the file created at 3;

This will facilitate the process of exporting and transfering by generating a much smaller file.

**Plane table**

A common data model provides a defined structure, usually encompassed in a sustainable environment to store and manage the data.
However, such a model can also raise the complexity of the representation when comparing to the most common formats used by researchers (mostly plane formats such as csv or spss) and present a more time consuming learning curve.

In the NCDC environment, it became important to also provide the harmonized data in a plane format as a starting point to interact with the federated infrastructure and as a faster pathway to analyse the data.
The command `parse-omop-to-plane` defines a new table based on the variable names from the mapping (`destination_mapping`) used.
This table is then populated with the data for each participant by visit from the database that follows the data model.
