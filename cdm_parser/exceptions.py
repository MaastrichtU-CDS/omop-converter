""" Custom exceptions.
"""

class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class ParsingError(Error):
    """Exception raised for errors while parsing the data.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
