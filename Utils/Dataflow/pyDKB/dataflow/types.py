"""
Type definitions for library objects.
"""

__all__ = ["dataType", "messageType", "codeType"]

from ..common import Type

dataType = Type("DOCUMENT", "AUTHOR", "DATASET")
messageType = Type("STRING", "JSON", "TTL")
codeType = Type("STRING")
