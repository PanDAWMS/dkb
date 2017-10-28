"""
Definition of common modules exceptions
"""

__all__ = ["HDFSException"]

class HDFSException(RuntimeError):
    """ Base Exception for HDFS module. """
    pass
