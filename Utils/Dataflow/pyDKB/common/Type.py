"""
Abstract class for type definitions.
"""


class Type(object):
    """
    Abstract class for type definitions.
    Usage:
        myType = Type("Orange", "Apple")

        myType.add("Plum")

        t = myType.Orange

        if t == myType.Orange:
            # Oranges stuff
        elif t == myType.member("Apple"):
            # Apples stuff
        ...

        if not myType.hasMember(t):
            print "Wrong type!"
    """

    def __init__(self, *args):
        self.ind = 1
        self.hash = {}
        for arg in args:
            self.add(arg)

    def add(self, name):
        """ Add new Type member.  """
        setattr(self, name, self.ind)
        self.hash[self.ind] = name
        self.ind += 1

    def member(self, name):
        """ Check if the member exists (by name).

        Return member value or False.
        """
        return getattr(self, name, False)

    def hasMember(self, val):
        """ Check if the member exists (by value). """
        return self.memberName(val) != False

    def memberName(self, val):
        """ Return string name of the member. """
        return self.hash.get(val, False)
