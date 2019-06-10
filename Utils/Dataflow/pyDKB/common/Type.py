"""
Abstract class for type definitions.

:Example:

>>> myType = Type("Orange", "Apple")
>>> myType.add("Plum")
>>> t = myType.Orange
>>> if t == myType.Orange:
...     print "Orange!"
... elif t == myType.member("Apple"):
...     print "Apple!"
...
Orange!
>>> if not myType.member("Walnut"):
...     print "Wrong type!"
...
Wrong type!
"""


class Type(object):
    """ Abstract class for type definitions.

    Member names (*str*) are passed to the constructor as positional arguments.
    """

    def __init__(self, *args):
        """ Initialize Type object. """
        self.ind = 1
        self.hash = {}
        for arg in args:
            self.add(arg)

    def add(self, name):
        """ Add member.

        :param name: name of the member to be added
        :type name: str
        """
        setattr(self, name, self.ind)
        self.hash[self.ind] = name
        self.ind += 1

    def member(self, name):
        """ Check if the member exists (by name).

        :param name: name to be checked
        :type name: str

        :return: member value or False if member does not exist
        :rtype: int, bool
        """
        return getattr(self, name, False)

    def hasMember(self, val):
        """ Check if the member exists (by value).

        :param val: member to be checked
        :type val: int

        :return: True/False
        :rtype: bool
        """
        return self.memberName(val) is not False

    def memberName(self, val):
        """ Return string name of the member.

        :param val: member to retrieve name for
        :type val: int

        :return: member name of False if member does not exist
        :rtype: str, bool
        """
        return self.hash.get(val, False)
