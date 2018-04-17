"""
Definition of abstract message class and specific message classes
"""

from . import messageType
from pyDKB.dataflow.types import codeType

import json
import sys

__message_class = {}


class DecodeUnknownType(NotImplementedError):
    """ Exception to be thrown when message type is not decodable. """
    def __init__(self, code, cls):
        message = "%s can`t be decoded from %s" \
                  % (cls.__name__, codeType.memberName(code))
        super(DecodeUnknownType, self).__init__(message)


class EncodeUnknownType(NotImplementedError):
    """ Exception to be thrown when message type is not encodable. """
    def __init__(self, code, cls):
        message = "%s can`t be encoded into %s" \
                  % (cls.__name__, codeType.memberName(code))
        super(EncodeUnknownType, self).__init__(message)


def Message(msg_type):
    """ Return class XXXMessage, where XXX is the passed type. """
    if not messageType.hasMember(msg_type):
        raise ValueError("Message type must be a member of messageType")
    cls = __message_class.get(msg_type)
    if not cls:
        sys.stderr.write(
            "(WARN) Message class for type %s is not implemented. "
            "Using AbstractMessage instead.")
        cls = AbstractMessage

    return cls


class AbstractMessage(object):
    """ Abstract message """

    msg_type = None
    native_types = []

    _ext = ".out"

    decoded = None
    encoded = None

    def __init__(self, message=None):
        """ Save initial message. """
        self.__orig = message
        if type(message) in self.native_types:
            self.decoded = message

    def getOriginal(self):
        """ Return original message. """
        return self.__orig

    def decode(self, code):
        """ Decode original from CODE to TYPE-specific format.

        Raises ValueError
        """
        raise DecodeUnknownType(code, self.__class__)

    def encode(self, code):
        """ Encode original message from TYPE-specific format to CODE.

        Raises ValueError
        """
        raise EncodeUnknownType(code, self.__class__)

    @classmethod
    def typeName(cls):
        """ Return message type name as string. """
        return messageType.memberName(cls.msg_type)

    def content(self):
        """ Return message content. """
        return self.decode()

    @classmethod
    def extension(cls):
        """ Return file extension corresponding this message type. """
        return cls._ext


class JSONMessage(AbstractMessage):
    """ Message in JSON format. """
    msg_type = messageType.JSON
    native_types = [dict]

    _ext = ".json"

    def decode(self, code=codeType.STRING):
        """ Decode original data as JSON. """
        if not self.decoded:
            orig = self.getOriginal()
            if code == codeType.STRING:
                self.decoded = json.loads(orig)
            else:
                raise DecodeUnknownType(code, self.__class__)
            self.encoded = orig
        return self.decoded

    def encode(self, code=codeType.STRING):
        """ Encode JSON as CODE. """
        if not self.encoded:
            orig = self.getOriginal()
            if code == codeType.STRING:
                self.encoded = json.dumps(orig)
            else:
                raise EncodeUnknownType(code, self.__class__)
            self.decoded = orig
        return self.encoded


__message_class[messageType.JSON] = JSONMessage


class TTLMessage(AbstractMessage):
    """ Messages in TTL format

    Single message = single TTL statement
    """
    msg_type = messageType.TTL

    try:
        native_types = [str, unicode]
    except NameError:
        native_types = [str]

    _ext = ".ttl"

    def decode(self, code=codeType.STRING):
        """ Decode original data as TTL.

        Currently takes text as it is.
        TODO: check some formal matter to confirm the string is TTL.
        """
        if not self.decoded:
            orig = self.getOriginal()
            if code == codeType.STRING:
                self.decoded = orig
            else:
                raise DecodeUnknownType(code, self.__class__)
            self.encoded = orig
        return self.decoded

    def encode(self, code=codeType.STRING):
        """ Encode TTL as CODE. """
        if not self.encoded:
            orig = self.getOriginal()
            if code == codeType.STRING:
                self.encoded = str(orig)
            else:
                raise EncodeUnknownType(code, self.__class__)
            self.decoded = orig
        return self.encoded


__message_class[messageType.TTL] = TTLMessage
