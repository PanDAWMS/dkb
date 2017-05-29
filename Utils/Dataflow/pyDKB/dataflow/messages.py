"""
Definition of abstract message class and specific message classes
"""

from . import messageType

import json

__message_class = {}

def Message(type):
    """ Return class XXXMessage, where XXX is the passed type. """
    if not messageType.hasMember(type):
        raise ValueError("Message type must be a member of messageType")
    cls = __message_class.get(type)
    if not cls:
        sys.stderr.write("(WARN) Message class for type %s is not implemented. "
                         "Using AbstractMessage instead.")
        cls = AbstractMessage

    return cls

class AbstractMessage(object):
    """ Abstract message """

    type = None

    def __init__(self, message=None):
        self.__orig = message
        self.message = None
        self.parse()

    def getOriginal(self):
        """ Return original message. """
        return self.__orig

    def parse(self):
        """ Verify original string format and parse.

        Raises ValueError
        """
        raise NotImplementedError("Method parse() is not implemented.")

    @classmethod
    def typeName(cls):
        """ Return message type name as string. """
        return messageType.memberName(cls.type)

    def content(self):
        """ Return message content. """
        return self.message

class JSONMessage(AbstractMessage):
    """ Message in JSON format. """
    type = messageType.JSON

    def parse(self):
        """ Parse original string as JSON. """
        if not self.message:
            try:
                orig = self.getOriginal()
                if type(orig) == dict:
                    self.message = orig
                else:
                    self.message = json.loads(orig)
            except ValueError, e:
                raise ValueError(e)
        return self.message

__message_class[messageType.JSON] = JSONMessage

class TTLMessage(AbstractMessage):
    """ Messages in TTL format

    Single message = single TTL statement
    """
    type = messageType.TTL

    def parse(self):
        """ Parse original string as TTL.

        Currently takes text as it is.
        TODO: check some formal matter to confirm the string is TTL.
        """
        if not self.message:
            self.message = self.__orig
        return self.message

__message_class[messageType.TTL] = TTLMessage
