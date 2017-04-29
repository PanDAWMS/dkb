__all__ = ["dkbID"]

import os
from . import dataType
from exceptions import DataflowException
from ..common.json_utils import valueByKey

import json

try:
  dir = os.path.dirname(__file__)
  CONFIG = json.load(file(os.path.join(dir,"dkbID.conf")))
  # Check required fields
  for src in CONFIG["document"]["priority"]:
    CONFIG["document"][src]["keywords"]
except IOError, e:
  raise DataflowException("Failed to read configuration for dkbID: %s" % e)
except ValueError, e:
  raise DataflowException("dkbID misconfigured: %s" % e)
except KeyError, e:
  raise DataflowException("dkbID misconfigured: \"%s\" is not defined." % e)

def firstValue(nestedList):
  """
  Returns first not None value from nested lists.
  """
  if type(nestedList) != list:
    return nestedList
  for val in nestedList:
    val = firstValue(val)
    if val: break
  return val

def docID(json_data):
  """
  json_data to contain information about just one document.
  """
  config = CONFIG["document"]
  val = None
  for source in config["priority"]:
    keywords = config[source]["keywords"]
    for key in keywords:
      val = valueByKey(json_data, key)
      if val: break
    if val: break

  val = firstValue(val)
  prefix = config[source].get("prefix", source)
  val = "{}_{}".format(prefix, val)

  return val

import uuid
def authorID(json_data):
  """
  TODO: get rid of meaningless UUID; use Surname_Name[_N] instead.
  """
  return str(uuid.uuid4())

# ----- #

__dataTypeFunc = {
    dataType.DOCUMENT:docID,
    dataType.AUTHOR:authorID
}

def dkbID(json_data, data_type):
  if type(json_data) != dict:
    raise DataflowException("dkbID() expects first argument of type %s (get %s)" % (dict, type(json_data)))

  func = __dataTypeFunc.get(data_type)
  if not func:
    raise DataflowException("Unknown data type in dkbID: %s" % data_type)

  return func(json_data)

