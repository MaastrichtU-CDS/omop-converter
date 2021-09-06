import os
import subprocess
from configparser import ConfigParser
from datetime import datetime
from dateutil.relativedelta import relativedelta

def import_config(path, section):
    """ Import the configurations from a file and set them as
        environment variables.
    """
    parser = ConfigParser()
    parser.read(path)

    if section in parser:
        for key, value in parser[section].items():
            os.environ[key.upper()] = value
    else:
        print('Section {0} not found in the configuration file'.format(section))

def export_config(path, section, configurations):
    """ Export the configuration variables to a file.
    """
    config = ConfigParser()
    config[section] = configurations

    with open(path, 'w') as configfile:
        config.write(configfile)

def run_command(command, success_message, error_message):
    """ Runs a bash command """
    process = subprocess.run(command, capture_output=True, check=False)
    if process.returncode == 0:
        print(success_message)
    else:
        print(error_message)
        print(process.stderr.decode("utf-8"))

def arrays_to_dict(keys, values):
    """ Convert two arrays (representing the keys and values) to a dictionary
    """
    return {keys[i]: values[i] for i in range(len(keys))}

def parse_date(date, input_format, output_format):
    """ Parse a date from one format to another.
    """
    date_parsed = datetime.strptime(date, input_format)
    return date_parsed.strftime(output_format)

def get_year_of_birth(age, date, input_format):
    """ Retrieve the year of birth from the age at a specific date.
    """
    date_parsed = datetime.strptime(date, input_format)
    return (date_parsed - relativedelta(years=age)).year
