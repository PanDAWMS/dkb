"""
Processor stages definitions (with predefined message type).
"""

__all__ = ["JSONProcessorStage", "TTLProcessorStage", "JSON2TTLProcessorStage"]

from . import AbstractProcessorStage
from . import messageType

import sys
import json


class JSONProcessorStage(AbstractProcessorStage):
    """ JSON2JSON Processor Stage

    Input message: JSON
    Output message: JSON
    """

    def __init__(self):
        super(JSONProcessorStage, self).__init__()
        self._set_input_message_class(messageType.JSON)
        self._set_output_message_class(messageType.JSON)

    def file_input(self, fd):
        """Override AbstractProcessorStage.file_input"""
        try:
            for m in self.file_nd_json(fd):
                yield m
        except ValueError:
            sys.stderr.write("(WARN) failed to read input file %(f)s as"
                             " Newline Delimeted %(t)s. Will try to read as"
                             " true %(t)s file.\n" % {"f": fd.name,
                                                      "t": self.input_message_class().typeName()})
            fd.seek(0)
            for m in self.file_true_json(fd):
                yield m

    def file_nd_json(self, fd):
        """ Read file as NDJSON file.

        Raises ValueError if can`t read the first line.
        """
        nd_ok = False
        data = self.stream_input(fd)
        m = data.next()
        if not (m and m.content() and type(m.content()) != list):
            raise ValueError
        yield m
        for m in data:
            yield m

    def file_true_json(self, fd):
        """ Read file as true JSON file. """
        try:
            data = json.load(fd)
            if type(data) == dict:
                data = [data]
            for m in data:
                yield self.parseMessage(m)
        except ValueError, err:
            sys.stderr.write("(WARN) failed to read input file %s as %s: %s.\n"
                             % (fd.name, self.input_message_class().typeName(), err))
            yield None


class TTLProcessorStage(AbstractProcessorStage):
    """ TTL2TTL Processor Stage

    Input message: TTL
    Output message: TTL
    """

    def __init__(self):
        super(JSONProcessorStage, self).__init__()
        self._set_input_message_class(messageType.JSON)
        self._set_output_message_class(messageType.JSON)

    # Override
    def output(self, message):
        super(TTLProcessorStage, self).output(message)


class JSON2TTLProcessorStage(JSONProcessorStage, TTLProcessorStage):
    """ JSON2TTL Procssor Stage

    Input message: JSON
    Output message: TTL
    """

    def __init__(self):
        # Skip direct ancestors (self, JSON, TTL)
        super(JSON2TTLProcessorStage.__mro__[2], self).__init__()
        self._set_input_message_class(messageType.JSON)
        self._set_output_message_class(messageType.TTL)

    def input(self):
        """Override: Falls back to JSONProcessorStage.input"""
        # Pick the method of the first parent
        return super(JSON2TTLProcessorStage, self).input()

    def output(self, message):
        """Override: Falls back to TTLProcessorStage.output"""
        # Skip first parent, pick the second
        super(JSONProcessorStage, self).output(message)
