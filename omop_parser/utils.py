import os
from configparser import ConfigParser

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
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

def export_config(path, section, configurations):
    """ Export the configuration variables to a file.
    """
    config = ConfigParser()
    config[section] = configurations

    with open(path, 'w') as configfile:
        config.write(configfile)
