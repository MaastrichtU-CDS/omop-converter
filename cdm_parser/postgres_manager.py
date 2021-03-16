import psycopg2
import os
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from constants import *

class PostgresManager:
    """ Manages the Postgres connection and methods to manipulate the database.
    """

    def __init__(self, default_db=False, isolation_level=None):
        if default_db:
            self.database = None
        else:
            self.database = os.getenv(DB_DATABASE)
        self.isConnected = False
        self.isolation_level = isolation_level
    
    def __enter__(self):
        """ Sets up the connection to the postgres database.
        """
        try:
            self.connection = psycopg2.connect(user=os.getenv(DB_USER),
                                            password=os.getenv(DB_PASSWORD),
                                            host=os.getenv(DB_HOST),
                                            port=os.getenv(DB_PORT),
                                            database=self.database)
            if self.connection:
                if self.isolation_level is not None:
                    self.connection.set_isolation_level(self.isolation_level)
                self.cursor = self.connection.cursor()
                self.isConnected = True
        except Exception as error:
            print('Error while connecting to PostgreSQL', error)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Wraps up the connection and other settings when exiting.
        """
        if self.isConnected:
            self.connection.close()
            self.cursor.close()

    def create_database(self, database_name):
        """ Create a new database.
        """
        self.cursor.execute('CREATE DATABASE "{}";'.format(database_name))
        self.connection.commit()

    def create_sequence(self, name):
        """ Create a new sequence.
        """
        self.cursor.execute('DROP SEQUENCE IF EXISTS {};'.format(name))
        self.cursor.execute('CREATE SEQUENCE IF NOT EXISTS {} AS BIGINT INCREMENT BY 1 START WITH 1;'.format(name))
        self.connection.commit()

    def run_sql(self, statement, returning=False):
        self.cursor.execute(statement)
        self.connection.commit()

        if returning:
            return self.cursor.fetchone()[0]

    def execute_file(self, path):
        """ Execute a file with a sql script.
        """
        self.cursor.execute(open(path, 'r').read())
        self.connection.commit()

    def copy_from_file(self, table, path):
        """ Insert data from a file.
        """
        with open(path, 'r') as data:
            self.cursor.copy_expert(f"COPY {table} FROM STDOUT WITH DELIMITER E'\t' NULL '' CSV HEADER QUOTE E'\b' ;", data)
            self.connection.commit()
