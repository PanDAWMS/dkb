"""
Processor stages definitions (with predefined message type).
"""

__all__ = ["JSONProcessorStage", "TTLProcessorStage", "JSON2TTLProcessorStage"]

from . import AbstractProcessorStage
from . import messageType

import sys
import json

class JSONProcessorStage(AbstractProcessorStage):
  """
  Input message: JSON
  Output message: JSON
  """

  def __init__(self):
    super(JSONProcessorStage, self).__init__()
    self._set_input_message_class(messageType.JSON)
    self._set_output_message_class(messageType.JSON)

  def file_input(self, fd):
    """Overrides AbstractProcessorStage.file_input"""
    try:
      data = json.load(fd)
      if type(data) == dict:
        data = [data]
      for m in data:
        yield self.parseMessage(m)
    except ValueError,e:
      sys.stderr.write("(WARN) failed to read input file %s as %s: %s.\n"
                          % (fd.name, self.input_message_class().typeName(), e))

class TTLProcessorStage(AbstractProcessorStage):
  """
  Input message: TTL
  Output message: TTL
  """

  def __init__(self):
    super(JSONProcessorStage, self).__init__()
    self._set_input_message_class(messageType.JSON)
    self._set_output_message_class(messageType.JSON)

  # Override
  def output(self, message):
    pass

class JSON2TTLProcessorStage(JSONProcessorStage,TTLProcessorStage):
  """
  Input message: JSON
  Output message: TTL
  """

  def __init__(self):
    # Get __init__ method of the last but one ancestor
    super(JSON2TTLProcessorStage.__mro__[-3], self).__init__()
    self._set_input_message_class(messageType.JSON)
    self._set_output_message_class(messageType.TTL)

  def input(self):
    """Override: Falls back to JSONProcessorStage.input"""
    # Pick the method of the first parent
    return super(JSON2TTLProcessorStage,self).input()

  def output(self,message):
    """Override: Falls back to TTLProcessorStage.output"""
    # Skip first parent, pick the second
    super(JSONProcessorStage,self).output(message)
