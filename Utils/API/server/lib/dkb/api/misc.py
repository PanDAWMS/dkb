""" Miscellaneous functions. """

from collections import defaultdict
import re


def sort_by_prefixes(values, prefixes, default=0):
    """ Classify values by 1-char prefixes.

    Generate hash with prefixes as keys and lists of values -- as values:
    ```
    { prefix: [val1, val2, ...], ... }
    ```
    If there are no values for some prefix, empty list is set as its value.

    :raise TypeError:

    :param values: values to be sorted
    :type values: list
    :param prefixes: allowed 1-char prefixes
    :type profixes: list
    :param default: id of the prefix to be used as a default one when none
                    specified. If set to None value without prefix will
                    produce a error.
    :type default: int, NoneType

    :return: hash with classified values
    :rtype: `collections.defaultdict(list)`
    """
    result = defaultdict(list)
    for v in values:
        try:
            if isinstance(v, str) and v[0] in prefixes:
                result[v[0]].append(v[1:])
            else:
                result[prefixes[default]].append(v)
        except IndexError:
            # Value is an empty string
            pass
    return result


def standardize_path(path):
    """ Bring path to a method to a standard view.

    Standerd view is:
     - starting with '/';
     - not ending with '/';
     - repeating '/' (like '///') replaced with single '/'.

    :param path: path to a method
    :type path: str

    :return: standardized path
    :rtype: str
    """
    path.rstrip('/')
    path = re.compile('/+').sub('/', '/' + path)
    return path
