"""
pyDKB.dataflow.communication.messages

Definition of abstract message class and specific message classes
"""

from . import messageType
from . import codeType
from pyDKB.common.misc import (log, logLevel)

import json
import sys
import copy

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
        log("Message class for type %s is not implemented. "
            "Using AbstractMessage instead.", logLevel.WARN)
        cls = AbstractMessage

    return cls


class AbstractMessage(object):
    """ Abstract message """

    msg_type = None
    native_types = []

    _ext = ".out"

    decoded = None
    encoded = None

    incompl = None

    def __init__(self, message=None):
        """ Save initial message. """
        self.__orig = message
        self.decode()

    def getOriginal(self):
        """ Return original message. """
        return copy.deepcopy(self.__orig)

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
        return copy.deepcopy(self.decode())

    @classmethod
    def extension(cls):
        """ Return file extension corresponding to this message type. """
        return cls._ext

    def incomplete(self, status=None):
        """ Set message incomplete marker and/or get previous/current value.

        :param status: new status (if not passed, current status is returned)
        :type status: bool, NoneType

        :return: incomplete marker status (previous value, if reset)
        :rtype: bool
        """
        old = self.incompl
        if status is not None:
            self.incompl = bool(status)
            self.encoded = None
        return old


class JSONMessage(AbstractMessage):
    """ Message in JSON format. """
    msg_type = messageType.JSON
    native_types = [dict, list, int, float]

    _ext = ".json"

    incompl_key = "_incomplete"

    _non_dict_not_implemented_warn = "JSON messages with non-dict content" \
                                     " are not fully implemented."

    def decode(self, code=codeType.STRING):
        """ Decode original data as JSON. """
        if not self.decoded:
            orig = self.getOriginal()
            if isinstance(orig, tuple(self.native_types)):
                self.decoded = orig
            elif code == codeType.STRING:
                self.decoded = json.loads(orig)
                self.encoded = orig
            else:
                raise DecodeUnknownType(code, self.__class__)
            if isinstance(self.decoded, dict):
                self.incomplete(self.decoded.pop(self.incompl_key, False))
            else:
                if self._non_dict_not_implemented_warn:
                    log(self._non_dict_not_implemented_warn, logLevel.WARN)
                    JSONMessage._non_dict_not_implemented_warn = None
                self.incomplete(False)
        return copy.deepcopy(self.decoded)

    def encode(self, code=codeType.STRING):
        """ Encode JSON as CODE. """
        if not self.encoded:
            content = self.content()
            if self.incomplete():
                if isinstance(content, dict):
                    content[self.incompl_key] = True
                else:
                    raise NotImplementedError("Incomplete marker for JSON"
                                              " message with non-dict content"
                                              " is not implemented.")
            if code == codeType.STRING:
                self.encoded = json.dumps(content)
            else:
                raise EncodeUnknownType(code, self.__class__)
            self.decoded = content
        return copy.deepcopy(self.encoded)


__message_class[messageType.JSON] = JSONMessage


class TTLMessage(AbstractMessage):
    """ Messages in TTL format

    Single message = single TTL statement
    """
    msg_type = messageType.TTL

    try:
        native_types = [str, str]
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
            if isinstance(orig, tuple(self.native_types)):
                self.decoded = orig
            elif code == codeType.STRING:
                self.decoded = orig
                self.encoded = orig
            else:
                raise DecodeUnknownType(code, self.__class__)
        return copy.deepcopy(self.decoded)

    def encode(self, code=codeType.STRING):
        """ Encode TTL as CODE. """
        if not self.encoded:
            orig = self.getOriginal()
            if code == codeType.STRING:
                self.encoded = str(orig)
            else:
                raise EncodeUnknownType(code, self.__class__)
            self.decoded = orig
        return copy.deepcopy(self.encoded)


__message_class[messageType.TTL] = TTLMessage
