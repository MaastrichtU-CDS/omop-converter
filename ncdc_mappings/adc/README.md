# Data Harmonization for ADC

The data for ADC is separated in 5 different files:
- Baseline (wide format) - baseline information (e.g., age)
- Diagnosis (long format)
- NPO (long format) - results for the neurocognitive assessment
- Visits (long format) - additional measurements and information (e.g., mmse score)
- SCD (long format) - plasma measurements

To transform this collection of datasets into an OMOP database:
1. Create the database, insert the vocabularies and constraints
```python
python3 cdm_parser_cli.py set-db
python3 cdm_parser_cli.py insert-voc
python3 cdm_parser_cli.py insert-constraints
```
2. Parse the data one dataset at the time, starting with the baseline (necessary to first create an entry for each participant in the `Person` table):
```python
export DATASET_PATH=/mnt/data/537_ADC_PHT_Wide_baseline.csv
python3 cdm_parser_cli.py parse-data --cohort-name adc
# Wait until all data has been processed
export DATASET_PATH=/mnt/data/537_ADC_PHT_Long_Visites.csv
python3 cdm_parser_cli.py parse-data --cohort-name adc
# Wait until all data has been processed
export DATASET_PATH=/mnt/data/537_ADC_PHT_Long_Diagnosis.csv
python3 cdm_parser_cli.py parse-data --cohort-name adc
# Wait until all data has been processed
export DATASET_PATH=/mnt/data/537_ADC_PHT_Long_NPO.csv
python3 cdm_parser_cli.py parse-data --cohort-name adc
# Wait until all data has been processed
export DATASET_PATH=/mnt/data/SCD_ADC_NCDC_voor_Justine_211028.csv
python3 cdm_parser_cli.py parse-data --cohort-name adc
# Wait until all data has been processed
# Check the report
python3 cdm_parser_cli.py report
# Create the ncdc table
python3 cdm_parser_cli.py parse-omop-to-plane --table-name ncdc
```

## Versions

- Cohort id 1 ("adc")
- Cohort id 2 ("adc_2025"): `sum` performed instead of the `mean` for the `priority_memory_im_15_word_list_correct`
