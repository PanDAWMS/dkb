"""
Utils to generate unique yet meaningful identifier for DKB objects.
"""

from . import dataType
from exceptions import DataflowException
from ..common.json_utils import valueByKey

import os
import json
import uuid

__all__ = ["dkbID"]


try:
    dir_ = os.path.dirname(__file__)
    CONFIG = json.load(file(os.path.join(dir_, "dkbID.conf")))
    # Check required fields
    for src in CONFIG["document"]["priority"]:
        CONFIG["document"][src]["keywords"]
except IOError, err:
    raise DataflowException("Failed to read configuration for dkbID: %s" % err)
except ValueError, err:
    raise DataflowException("dkbID misconfigured: %s" % err)
except KeyError, err:
    raise DataflowException(
        "dkbID misconfigured: \"%s\" is not defined." % err)


def firstValue(nestedList):
    """ Return first not None value from nested lists. """
    if type(nestedList) != list:
        return nestedList
    val = None
    for val in nestedList:
        val = firstValue(val)
        if val:
            break
    return val


def docID(json_data):
    """ Return unique identifier for Document DKB object.

    Parameters:
        DICT json_data -- contain information about just one document.
    """
    config = CONFIG["document"]
    val = None
    source = None
    for source in config["priority"]:
        keywords = config[source]["keywords"]
        for key in keywords:
            val = valueByKey(json_data, key)
            if val:
                break
        if val:
            break

    val = firstValue(val)
    if source:
        prefix = config[source].get("prefix", source)
        val = "{}_{}".format(prefix, val)

    return val


def authorID(json_data):
    """ Return unique identifier for Author DKB object.

    TODO: get rid of meaningless UUID; use Surname_Name[_N] instead.
    """
    return str(uuid.uuid4())

# ----- #


__dataTypeFunc = {
    dataType.DOCUMENT: docID,
    dataType.AUTHOR: authorID
}


def dkbID(json_data, data_type):
    """ Return unique identifier for object of TYPE based on DATA. """
    if type(json_data) != dict:
        raise DataflowException("dkbID() expects first argument of type %s "
                                "(get %s)" % (dict, type(json_data)))

    func = __dataTypeFunc.get(data_type)
    if not func:
        raise DataflowException("Unknown data type in dkbID: %s" % data_type)

    return func(json_data)
