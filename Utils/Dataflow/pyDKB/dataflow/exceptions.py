"""
Definition of DKB Dataflow exceptions
"""

__all__ = ["DataflowException"]


class DataflowException(Exception):
    """ Base Exception for Dataflow modules. """
    reason = None

    def __init__(self, message='', reason=None):
        """ Initialise exception instance. """
        super(DataflowException, self).__init__(message)
        self.reason = reason

    def __str__(self):
        """ Cast exception to string. """
        msg = super(DataflowException, self).__str__()
        prefix = ''
        if msg:
            prefix = '\nReason: '
        if self.reason:
            msg += prefix + str(self.reason)
        return msg
