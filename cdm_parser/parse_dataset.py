from operator import le, lt, ge, gt

import csv
import pandas as pd

from cdm_builder import *
from constants import *
from exceptions import ParsingError
from utils import arrays_to_dict, parse_date, get_year_of_birth, parse_float, is_value_valid

CDM_SQL = {
    CONDITION_OCCURRENCE: {
        BUILD: build_condition,
        CHECK_DUPLICATE: check_duplicated_condition,
    },
    MEASUREMENT: {
        BUILD: build_measurement,
        CHECK_DUPLICATE: check_duplicated_measurement,
    },
    OBSERVATION: {
        BUILD: build_observation,
        CHECK_DUPLICATE: check_duplicated_observation,
    },
}

CDM_SQL_VALUES = {
    CONDITION_OCCURRENCE: {
        BUILD: build_condition_values,
        BULK: build_condition_bulk,
    },
    MEASUREMENT: {
        BUILD: build_measurement_values,
        BULK: build_measurement_bulk,
    },
    OBSERVATION: {
        BUILD: build_observation_values,
        BULK: build_observation_bulk,
    },
}

class DataParser:
    """ Parses the dataset to the OMOP CDM.
    """
    def __init__(self, source_mapping, destination_mapping,
        fu_suffix, fu_prefix, cohort_id, missing_values, ignore_duplicate, pg):
        self.source_mapping = source_mapping
        self.destination_mapping = destination_mapping
        self.cohort_id = cohort_id
        self.ignore_duplicate = ignore_duplicate
        self.pg = pg
        self.warnings = []

        # Keywords used as missing values
        self.missing_values = missing_values.split(';') if missing_values else []

        # Build the array with the follow up suffixes and prefixes
        # An empty string is included to represent the baseline (
        # variables that do not include a prefix/suffix)
        self.fu_suffix = ['']
        if fu_suffix:
            self.fu_suffix.extend(fu_suffix.split(DEFAULT_SEPARATOR))
        self.fu_prefix = []
        if fu_prefix:
            self.fu_prefix.extend(fu_prefix.split(DEFAULT_SEPARATOR))
        # Retrieve the necessary information from the mappings
        self.value_mapping = self.create_value_mapping()
        (self.date_source_variables, self.date_format, _) = self.get_parameters(DATE, with_format=True)

    @staticmethod
    def variable_values_to_dict(keys, values, separator=DEFAULT_SEPARATOR):
        """ Convert the string representing the variable values map to a dictionary.
        """
        return arrays_to_dict(
            keys.split(separator),
            values.split(separator)
        )

    @staticmethod
    def validate_value(value, validation):
        """ Validate if a value is within the expected range.
        """
        validation_functions = {
            '>=': ge,
            '<=': le,
            '>': gt,
            '<': lt,
        }
        for key, condition in validation_functions.items():
            if key in validation:
                val_condition = validation.split(key)
                return condition(parse_float(value), parse_float(val_condition[1]) if len(val_condition) > 1 else 0)
        return True

    @staticmethod
    def create_variable_names(variable, prefixes, suffixes):
        variable_names = []
        for prefix in prefixes:
            variable_names.append(prefix + variable)
        for suffix in suffixes:
            variable_names.append(variable + suffix)
        return variable_names

    @staticmethod
    def valid_row_value(variable, row, ignore_values=[], validation=None, limit=None):
        """ Validate if the value exists and is not null
        """
        # Validation performed:
        # - the variable is present in the row
        # - the value is not null or an empty string
        # - the value contains a symbol (e.g. >=)
        # - the value must be below a certain limit
        # - the value must be within a certain range (can be categorical or numerical)
        return variable in row and is_value_valid(row[variable]) and \
            str(row[variable]) not in ignore_values and \
                (len([symbol in str(row[variable]) for symbol in SYMBOLS_CONCEPT_ID.keys()]) > 0 or \
                    (not is_value_valid(limit) or parse_float(row[variable]) < parse_float(limit)) and \
                        (not validation or DataParser.validate_value(row[variable], validation)))

    @staticmethod
    def parse_dataset(path, start, limit, convert_categoricals, delimiter, callback, bulk=False, bulk_range=1):
        """ Read the dataset according to the file type
        """
        error_handling = 'ignore' if os.getenv(IGNORE_ENCODING_ERRORS) else 'strict'
        header = None
        kwargs = {
            'start': start,
            'limit': limit,
            'bulk': bulk,
            'bulk_range': int(bulk_range),
        }
        if '.csv' in path:
            with open(path, 'r', errors=error_handling, encoding=os.getenv(ENCODING)) as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
                for i in range(start):
                    next(csv_reader)
                callback(enumerate(csv_reader, start=start), **kwargs)
                header = csv_reader.fieldnames
            # Alternative:
            # df = pd.read_csv(path, encoding=os.getenv(ENCODING), on_bad_lines='skip', delimiter=delimiter)
            # callback(df.loc[start:].iterrows(), **kwargs)
            # header = df.head()
        elif '.sav' in path:
            df = pd.read_spss(path, convert_categoricals=convert_categoricals)
            callback(df.loc[start:].iterrows(), **kwargs)
            header = df.head()
        elif '.sas' in path:
            df = pd.read_sas(path, encoding=os.getenv(ENCODING))
            callback(df.loc[start:].iterrows(), **kwargs)
            header = df.head()
        return header

    @staticmethod
    def parse_source_value(source_values):
        """ Parse the source value to keep store it in the DB.
        """
        # TODO: create an environment variable to constraint the string size.
        return (';'.join([str(value) for value in source_values]))[:50]

    def map_variable_values(self, variable, specification):
        """ Create the mapping between a source and destination variable
        """
        mapping = {}
        source_values = self.variable_values_to_dict(
            specification[VALUES],
            specification[VALUES_PARSED]
        )
        if self.destination_mapping[variable][VALUES_CONCEPT_ID]:
            # Mapping each value to the concept ID defined
            destination_values = self.variable_values_to_dict(
                self.destination_mapping[variable][VALUES],
                self.destination_mapping[variable][VALUES_CONCEPT_ID]
            )
            destination_values[DEFAULT_SKIP] = DEFAULT_SKIP
            mapping = {map_key: destination_values[map_value] \
                for map_key, map_value in source_values.items()}
            mapping[VALUE_AS_CONCEPT_ID] = True
        else:
            # Mapping each value to another string
            mapping = source_values
            mapping[VALUE_AS_CONCEPT_ID] = False
        return mapping
    
    def create_value_mapping(self):
        """ Create the mapping between the source and destination values for each variable
        """
        value_mapping = {}
        for key, value in self.source_mapping.items():
            if value[VALUES]:
                if not value[VALUES_PARSED]:
                    raise ParsingError(f'Error in the source mapping for variable {key}')
                # TODO: VALUES_CONCEPT_ID not in self.destination_mapping[key] can be removed?
                if key not in self.destination_mapping or VALUES_CONCEPT_ID not in self.destination_mapping[key]:
                    raise ParsingError(f'Variable {key} is not correctly mapped in the destination mapping!')
                try:
                    value_mapping[key] = self.map_variable_values(key, value)
                except Exception as error:
                    raise ParsingError(f'Error creating the value mapping for variable {key}: {str(error)}')
        return value_mapping

    def get_parameters(self, parameter, with_format=False):
        """ Returns the source variable and format for a parameter.
        """
        parameter_source_variables = None
        parameter_format = None
        limit = None
        if parameter and parameter in self.source_mapping:
            parameter_source_variables = [self.source_mapping[parameter][SOURCE_VARIABLE]]
            if self.source_mapping[parameter][ALTERNATIVES]:
                parameter_source_variables.extend(
                    self.source_mapping[parameter][ALTERNATIVES].split(DEFAULT_SEPARATOR))
            if self.source_mapping[parameter][FORMAT]:
                parameter_format = self.source_mapping[parameter][FORMAT]
            elif with_format:
                raise ParsingError(f'Format required for variable: {parameter}')
            if is_value_valid(self.source_mapping[parameter][LIMIT]):
                limit = self.source_mapping[parameter][LIMIT]
        return (parameter_source_variables, parameter_format, limit)

    def get_parsed_value(self, variable, value, aggregate=None, conversion=None, threshold=None, source_variable=None,
        format=None, type=None, prefix=None, suffix=None):
        """ Get the parsed value for a variable.
        """
        # TODO: convoluted function, too many return statements
        # Convert symbols to standard codes
        symbol_cid = None
        value_parsed = value
        for symbol in SYMBOLS_CONCEPT_ID.keys():                            
            if symbol in str(value):
                value_parsed = str(value).split(symbol, 1)[1]
                symbol_cid = SYMBOLS_CONCEPT_ID[symbol]
                break
        # Parse the value according to the case:
        # - Thresholding the value and obtaining a boolean
        # - Using the value without transformations
        value_parsed = parse_float(value_parsed) > parse_float(threshold) \
            if threshold else value_parsed
        if variable in self.value_mapping:
            # Retrieving the destination value from the mapping
            value_map = self.value_mapping[variable]
            concept_id = self.value_mapping[variable][VALUE_AS_CONCEPT_ID]
            value_code = None
            if str(value_parsed) in value_map:
                value_code =  value_map[str(value_parsed)]
            elif DEFAULT_VALUE in value_map:
                value_code = value_map[DEFAULT_VALUE]
            elif source_variable:
                if source_variable in value_map:
                    value_code = value_map[source_variable]
                else:
                    source_value_stripped = source_variable.strip(prefix).strip(suffix)
                    if source_value_stripped in value_map:
                        value_code = value_map[source_value_stripped]
            if value_code is None:
                raise ParsingError(f'Variable {variable} is incorrectly mapped: value {value} is not mapped')
            return (concept_id, value_code, symbol_cid)
        elif aggregate:
            # Aggregating multiple values using one of the aggregation functions available.
            aggregated_value = None
            values = [parse_float(val) * parse_float(conversion) for val in value] if conversion \
                else [parse_float(val) for val in value]
            if aggregate == MEAN:
                aggregated_value = sum(values)/len(value)
            elif aggregate == SUM:
                aggregated_value = sum(values)
            else:
                raise ParsingError(f'Unrecognized function {aggregate} to aggregate the values for variable {variable}')
            return (False, aggregated_value, symbol_cid)
        # If any of the previous cases don't apply, return the value and apply a conversion if necessary.
        if type == TYPE_DATE:
            if format:
                try:
                    value_parsed = parse_date(value_parsed, format, POSTGRES_DATE_FORMAT)
                except Exception as error:
                    raise ParsingError(f"Failed to parse date {variable} with value {value} from format " +
                        f"{format} to {POSTGRES_DATE_FORMAT}: {str(error)}")
            else:
                raise ParsingError(f"No date format provided for variable {variable}!")
        elif type == TYPE_NUMERIC:
            value_parsed = parse_float(value_parsed)
        return (False, parse_float(value_parsed) * parse_float(conversion) if conversion else value_parsed, symbol_cid)

    def get_death_datetime(self, row):
        """ Retrieve the death datetime if available. Otherwise, if a
            flag is present, a default value will be used.
        """
        death_datetime = None
        death_time_source_variables = self.get_source_variable_from_wave(DEATH_DATE)
        death_flag_source_variables = self.get_source_variable_from_wave(DEATH_FLAG)
        for death_time_source_variable in death_time_source_variables:
            if death_time_source_variable and self.valid_row_value(
                death_time_source_variable, row, ignore_values=self.missing_values
            ):
                try:
                    # TODO: get death date format first
                    death_datetime = parse_date(str(row[death_time_source_variable]), self.date_format, DATE_FORMAT)
                    break
                except Exception as error:
                    raise ParsingError(
                        f'Error parsing a malformated date for the death date {death_time_source_variable} ' + 
                        f'with source variable {str(row[death_time_source_variable])}: {str(error)})'
                    )
        if death_datetime is None:
            for death_flag_source_variable in death_flag_source_variables:
                if death_flag_source_variable and self.valid_row_value(
                    death_flag_source_variable, row, ignore_values=self.missing_values
                ):
                    (_, parsed_value, _) = self.get_parsed_value(DEATH_FLAG, row[death_flag_source_variable])
                    if parsed_value and parsed_value == 'True':
                        death_datetime = DATE_DEFAULT
        return death_datetime

    def get_source_variable(self, variable):
        """ Check if there is a map for the source id.
        """
        return self.source_mapping[variable][SOURCE_VARIABLE] if variable in self.source_mapping else None

    def get_source_variable_from_wave(self, variable):
        """ Check if there is a map for the source id.
        """
        source_variable = self.get_source_variable(variable)
        return (self.create_variable_names(source_variable, self.fu_prefix, self.fu_suffix))
    
    def parse_person(self, row):
        """ Parse the person information from the row.
        """
        sex_source_variables = self.get_source_variable_from_wave(GENDER)
        sex_source_variable = None
        for source_variable in sex_source_variables:
            if self.valid_row_value(source_variable, row):
                # TODO: data quality check (all values match?)
                sex_source_variable = source_variable
                break
        if sex_source_variable is None:
            raise ParsingError('Missing information for the sex variable.')
        birth_year_source_variables = self.get_source_variable_from_wave(YEAR_OF_BIRTH)
        birth_year = None
        for source_variable in birth_year_source_variables:
            if self.valid_row_value(source_variable, row):
                birth_year = int(parse_float(row[source_variable]))
                break
        if birth_year is None:
            # The year of birth is required to create an entry for the person. In case that 
            # variable isn't provided, the year of birth will be obtained from a variable indicating
            # the age for a particular date.
            if AGE in self.destination_mapping:
                (age_variables, _, limit) = self.get_parameters(AGE)
                if age_variables:
                    (age_date_variables, age_date_format, _) = self.get_parameters(
                            self.destination_mapping[AGE][DATE])
                    for i, age_variable_base in enumerate(age_variables):
                        # age_variable_wave = self.get_source_variable_from_wave(age_variable_base)
                        age_variable_wave = self.create_variable_names(age_variable_base, self.fu_prefix, self.fu_suffix)
                        if age_date_variables and len(age_date_variables) > i:
                            age_date_variable_wave = self.create_variable_names(age_date_variables[i], self.fu_prefix, self.fu_suffix)
                            for j, age_variable in enumerate(age_variable_wave):
                                age = None
                                if self.valid_row_value(age_variable, row, limit=limit) and age_date_variables:
                                    age = parse_float(row[age_variable])
                                elif AGE in self.source_mapping and is_value_valid(self.source_mapping[AGE][STATIC_VALUE]):
                                    age = parse_float(self.source_mapping[AGE][STATIC_VALUE])
                                if is_value_valid(age):
                                    try:
                                        birth_year = get_year_of_birth(age, str(row[age_date_variable_wave[j]]), \
                                            age_date_format if age_date_format else self.date_format)
                                        break
                                    except Exception as error:
                                        raise ParsingError(
                                            f'Error parsing year of birth from variable {age_variable}: {str(error)}')
                        else:
                            raise ParsingError(
                                            f'Error parsing year of birth, date variable not found {age_variable}: {str(error)}')

        if not birth_year:
            raise ParsingError('Missing required information, the row should contain the year of birth.')

        # Add a new entry for the person/patient
        person_sql = build_person(
            self.get_parsed_value(GENDER, row[sex_source_variable])[1],
            birth_year,
            self.cohort_id,
            self.get_death_datetime(row),
        )
        person_id = self.pg.run_sql(*person_sql, fetch_one=True)

        return person_id

    def update_person(self, person_id, row):
        """ Update a person if new information is available.
        """
        death_datetime = self.get_death_datetime(row)
        if death_datetime:
            self.pg.run_sql(*update_person(person_id, death_datetime))

    def get_visits(self, row, person_id, suffix='', prefix=''):
        """ Retrieve existing visit dates or parse the available dates and
            create new visits. A suffix can be provided to get/parse 
            visits for the follow ups.
        """
        # Parse the date for the observation/measurement/condition if available
        # TODO: Calculating the end data when provided with a period for the wave
        visits = {}
        for date_source_variable in self.date_source_variables:
            date_variable = prefix + date_source_variable + suffix
            if date_variable in row and row[date_variable] and is_value_valid(row[date_variable]):
                visit_id = None
                try:
                    visit_date = parse_date(str(row[date_variable]), self.date_format, DATE_FORMAT)
                    visit_id = get_visit_by_person_and_date(self.pg, person_id, visit_date)
                    if not visit_id:
                        visit_id = insert_visit_occurrence(person_id, visit_date, visit_date, self.cohort_id, self.pg)
                    visits[date_variable] = visit_id
                except Exception as error:
                    print(f"Error while trying to parse a date from the following variable {date_variable}" + \
                          f"(person id: {person_id}): {str(error)}")
        return visits

    def transform_rows(self, iterator, start, limit, bulk=False, bulk_range=50):
        """ Transform each row in the dataset
        """
        id_map = {}
        processed_records = 0
        skipped_records = 0
        bulk_insert_records = 0
        id_source_variable = self.get_source_variable(SOURCE_ID)
        if not id_source_variable:
            print("No source id variable provided!")
        insert_statements = {
            OBSERVATION: [],
            MEASUREMENT: [],
            CONDITION_OCCURRENCE: [],
        }
        for index, row in iterator:
            if limit > 0 and index - start >= limit:
                break
            try:
                # Check if the source id variable is provided. In that case,
                # the link between the source id and the person id will be stored
                # in a dictionary and in a temporary table.
                person_id = None
                # TODO: also provide the source id when there is an error
                if id_source_variable:
                    if not self.valid_row_value(id_source_variable, row):
                        raise ParsingError(
                            f'Error when parsing the source id ({id_source_variable}) for record number {index}.')
                    source_id = row[id_source_variable]
                    if source_id in id_map:
                        person_id = id_map[source_id]
                        self.update_person(person_id, row)
                    else:
                        # First check if it's already included in the temporary table.
                        person_id = get_person_id(source_id, self.cohort_id, self.pg)
                        if person_id:
                            self.update_person(person_id, row)
                        else:
                            person_id = self.parse_person(row)
                            insert_id_record(source_id, person_id, self.cohort_id, self.pg)
                        id_map[source_id] = person_id
                else:
                    # print('No ID variable available')
                    person_id = self.parse_person(row)
                # Parse the row once for each prefix/suffix used
                visit_found = False
                fu_info = {
                    PREFIX: self.fu_prefix,
                    SUFFIX: self.fu_suffix,
                }
                for info_key, info_values in fu_info.items():
                    for info_value in info_values:
                        prefix=info_value if info_key == PREFIX else ''
                        suffix=info_value if info_key == SUFFIX else ''
                        # Retrieve the visit or insert a new visit for the participant
                        visits = self.get_visits(
                            row,
                            person_id,
                            prefix=prefix,
                            suffix=suffix,
                        )
                        if len(visits.keys()) > 0:
                            visit_found = True
                            # Process the data in the row. If bulk is True, it creates the sql statements by
                            # OMOP domain. If bulk is False, it will insert each variable individually.
                            sql_statements = self.transform_row(
                                row,
                                person_id,
                                visits,
                                prefix=prefix,
                                suffix=suffix,
                                bulk=bulk,
                            )
                            if bulk:
                                for (sql_domain, sql_statement) in sql_statements:
                                    bulk_insert_records += 1
                                    insert_statements[sql_domain].append(sql_statement)
                if not visit_found:
                    print(f'No visit dates found for the person with id {person_id}')
                # Keep track of the number of records processed
                processed_records += 1
                if processed_records % 250 == 0:
                    print(f'Processed {processed_records} records')
                if bulk and bulk_insert_records > bulk_range:
                    # print(f"Bulk insert: {bulk_insert_records} records")
                    for sql_domain in insert_statements.keys():
                        # print(f"Domain {sql_domain}")
                        for record_count in range(0, len(insert_statements[sql_domain]), bulk_range):
                            self.pg.run_sql(
                                CDM_SQL_VALUES[sql_domain][BULK](
                                    insert_statements[sql_domain][record_count: record_count + bulk_range]
                                )
                            )
                        insert_statements[sql_domain] = []
                    bulk_insert_records = 0
            except ParsingError as error:
               # TODO: Use a logger and add this information in a file
               print(f'Skipped record {index} due to an error: {str(error)}')
               skipped_records += 1
        print(f'Processed {processed_records} records and skipped {skipped_records} records due to errors')

    def transform_row(self, row, person_id, visits, prefix='', suffix='', bulk=False):
        """ Transform each row and insert in the database.
        """
        # Parse the observations/measurements/conditions
        sql_statements = []
        for key, value in self.source_mapping.items():
            if key not in self.destination_mapping:
                if DATE not in key.lower() and key not in self.warnings:
                    print(f'Skipped variable {key} since its not mapped')
                    self.warnings.append(key)
            elif self.destination_mapping[key][DOMAIN] not in CDM_SQL:
                if self.destination_mapping[key][DOMAIN] not in [PERSON, NOT_APPLICABLE] and \
                    key not in self.warnings:
                    print(f'Skipped variable {key} since its domain is not currently accepted')
                    self.warnings.append(key)
            else:
                # TODO: Improve the logic for the Condition column
                source_value = []
                source_variable_valid = []
                source_value_alternative = []
                source_variable_valid_alternative = []
                if value[SOURCE_VARIABLE]:
                    source_variables = [value[SOURCE_VARIABLE]]
                    if value[ALTERNATIVES]:
                        source_variables.extend(value[ALTERNATIVES].split(DEFAULT_SEPARATOR))
                    # Check the first variable for the field that it's valid
                    for source_variable in source_variables:
                        source_variable_suffixed = prefix + source_variable + suffix
                        # Validate source value by checking if it's not null, not a missing value, 
                        # and (if provided) apply a condition.
                        if self.valid_row_value(
                            source_variable_suffixed,
                            row,
                            ignore_values=self.missing_values,
                            validation=self.destination_mapping[key][VALUES_RANGE],
                            limit=value[LIMIT]
                        ):
                            if not is_value_valid(value[CONDITION]) or row[source_variable_suffixed] \
                                in value[CONDITION].split(DEFAULT_SEPARATOR):
                                    source_value.append(row[source_variable_suffixed])
                                    source_variable_valid.append(source_variable_suffixed)
                            else:
                                source_value_alternative.append(row[source_variable_suffixed])
                                source_variable_valid_alternative.append(source_variable_suffixed)
                    if len(source_value) == 0:
                        source_value = source_value_alternative
                        source_variable_valid = source_variable_valid_alternative
                # TODO: Also used for additonal information added to a different column
                # probably better to separate the two.
                elif value[STATIC_VALUE]:
                    source_value = [value[STATIC_VALUE]]
                if len(source_value) > 0:
                    # TODO: improve the mapping between a variable and multiple
                    # source variables
                    try:
                        domain = self.destination_mapping[key][DOMAIN]
                        (value_as_concept, parsed_value, symbol_cid) = self.get_parsed_value(
                            key,
                            source_value if value[AGGREGATE] else source_value[0],
                            aggregate=value[AGGREGATE],
                            conversion=value[CONVERSION],
                            threshold=value[THRESHOLD],
                            source_variable=source_variable_valid[0] if len(source_variable_valid) > 0 else None,
                            format=value[FORMAT],
                            type=self.destination_mapping[key][TYPE],
                            prefix=prefix,
                            suffix=suffix,
                        )
                        if parsed_value != DEFAULT_SKIP:
                            # Check if there is a specific date for the variable
                            date = DATE_DEFAULT
                            visit_id = visits[list(visits.keys())[0]]
                            (source_dates, source_date_format, _) = self.get_parameters(
                                self.destination_mapping[key][DATE])
                            if source_dates:
                                for source_date in source_dates:
                                    source_date_variable = prefix + source_date + suffix
                                    if source_date_variable in visits:
                                        visit_id = visits[source_date_variable]
                                    if self.valid_row_value(source_date_variable, row, ignore_values=self.missing_values):
                                        try:
                                            date = parse_date(
                                                str(row[source_date_variable]),
                                                source_date_format or self.date_format,
                                                DATE_FORMAT,
                                            )
                                            break
                                        except Exception as error:
                                            raise ParsingError(
                                                f'Error parsing a malformated date for variable {key} \
                                                    with source variable {source_date_variable}: {str(error)})'
                                            )
                            # Create the necessary arguments to build the SQL statement
                            named_args = {
                                'source_value': self.parse_source_value(source_value),
                                'date': date,
                                'visit_id': visit_id,
                                'symbol_cid': symbol_cid
                            }
                            if value_as_concept:
                                named_args['value_as_concept'] = parsed_value
                            else:
                                named_args['value'] = parsed_value
                            # Check if there is a field for additional information
                            additional_info = self.destination_mapping[key][ADDITIONAL_INFO]
                            if additional_info and additional_info in self.source_mapping:
                                additional_info_value = self.source_mapping[additional_info][STATIC_VALUE]
                                if additional_info_value:
                                    named_args['additional_info'] = additional_info_value
                                else:
                                    additional_info_variable = prefix + self.source_mapping[additional_info][SOURCE_VARIABLE] + suffix
                                    if additional_info_variable and self.valid_row_value(additional_info_variable, row):
                                        additional_info_value = self.get_parsed_value(
                                            additional_info_variable,
                                            row[additional_info_variable],
                                            source_variable=additional_info_variable
                                        )[1]
                                        named_args['additional_info'] = f'{additional_info_variable}: {additional_info_value}'
                            elif value[STATIC_VALUE]:
                                named_args['additional_info'] = value[STATIC_VALUE]

                            # Run the SQL script to insert the measurement/observation/condition
                            # if not self.ignore_duplicate or not self.pg.run_sql(
                            #     *CDM_SQL[domain][CHECK_DUPLICATE](person_id, self.destination_mapping[key], **named_args),
                            #     fetch_one=True
                            # ):
                            if bulk:
                                sql_statements.append((domain, CDM_SQL_VALUES[domain][BUILD](person_id, self.destination_mapping[key], **named_args)))
                            else:
                                self.pg.run_sql(*CDM_SQL[domain][BUILD](person_id, self.destination_mapping[key], **named_args))
                    except ParsingError as error:
                        if key not in self.warnings:
                            self.warnings.append(key)
                            print(f"Error when transforming the row for variable {key}: {error}")
                        pass
        return sql_statements
