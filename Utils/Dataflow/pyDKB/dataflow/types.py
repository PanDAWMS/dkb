"""
Type definitions for library objects.
"""

from ..common import Type

__all__ = ["dataType", "messageType", "codeType"]

dataType = Type("DOCUMENT", "AUTHOR", "DATASET")
messageType = Type("STRING", "JSON", "TTL")
codeType = Type("STRING")
