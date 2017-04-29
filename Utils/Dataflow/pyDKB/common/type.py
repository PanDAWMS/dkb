"""
Abstrac class for type definitions.
"""

class Type(object):

  def __init__(self, *args):
    self.ind = 1
    self.hash = {}
    for arg in args:
      self.add(arg)

  def add(self, name):
    setattr(self, name, self.ind)
    self.hash[self.ind] = name
    self.ind += 1

  def member(self, name):
    return getattr(self, name, False)

  def memberName(self, val):
    return self.hash.get(val, False)

  def hasMember(self, val):
    return self.memberName(val) != False
