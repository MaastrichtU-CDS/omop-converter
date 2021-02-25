from constants import CONCEPT

def get_person(gender, year_of_birth):
    """ Build the sql statement for a person.
    """
    gender_concept_id = None
    if any (gender.lower() in male_gender_code for male_gender_code in ['m', 'male']):
        gender_concept_id = 4059166
    elif any (gender.lower() in male_gender_code for male_gender_code in ['f', 'female']):
        gender_concept_id = 4019837

    return """INSERT INTO PERSON (person_id,gender_concept_id,year_of_birth,
        race_concept_id,ethnicity_concept_id,gender_source_concept_id,race_source_concept_id,ethnicity_source_concept_id)
        VALUES (nextval('person_sequence'),{0},{1},0,0,0,0,0)
        RETURNING person_id;
    """.format(gender_concept_id, year_of_birth)

def get_observation(value, person_id, field):
    """ Build the sql statement for an observation.
    """
    return """INSERT INTO OBSERVATION (observation_id,person_id,observation_concept_id,observation_datetime,
        observation_type_concept_id,value_as_string,observation_source_value,observation_source_concept_id,
        obs_event_field_concept_id) VALUES (nextval('observation_sequence'), {0}, {1},
        '19700101 00:00:00', 32879, {2}, {2}, 0, 0);
    """.format(person_id, field[CONCEPT], value)

def get_measurement(value, person_id, field):
    """ Build the sql statement for a measurement.
    """
    return """INSERT INTO MEASUREMENT (measurement_id,person_id,measurement_concept_id,measurement_datetime,
        measurement_type_concept_id,value_as_number,measurement_source_concept_id)
        VALUES (nextval('measurement_sequence'), {0}, {1}, '19700101 00:00:00', 0, {2}, 0)
    """.format(person_id, field[CONCEPT], value)

def get_condition(value, person_id, field):
    """ Build the sql statement for a condition.
    """
    return """INSERT INTO CONDITION_OCCURRENCE (condition_occurrence_id,person_id,condition_concept_id,
        condition_start_datetime,condition_type_concept_id,condition_status_concept_id,condition_source_concept_id)
        VALUES (nextval('condition_sequence'), {0}, {1}, '19700101 00:00:00', 0, 0, 0)
    """.format(person_id, field[CONCEPT])
