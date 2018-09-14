"""
Utils to work with JSON objects.

In context of Python, JSON [#]_ objects may be considered as structures
consisting of six types of elements:

- dictionaries,
- lists,
- strings,
- numbers,
- True/False,
- Null.

DKB project uses JSON for storing various information and transferring it
between stages. This module contains functions which simplify some aspects
of retrieving data from JSON objects.

.. [#] https://www.json.org/

"""


def valueByKey(json_data, key):
    """ Return value by a chain (list) of nested keys.

    It is common for JSON objects to contain many layers of dictionaries
    nested in other dictionaries -- this function extracts the data from
    such constructions according to given string or list with keys.

    :param json_data: to search in
    :type json_data: dict
    :param key: nested keys
    :type key: str, list

    :return: value (None if failed)
    :rtype: depends on value, NoneType
    """
    nested_keys = nestedKeys(key)
    val = json_data
    for i in range(len(nested_keys)):
        k = nested_keys[i]
        val = val.get(k)
        if not val:
            return None
        if type(val) == list:
            values = []
            for j in range(len(val)):
                values.append(valueByKey(val[j], nested_keys[i + 1:]))
            return values
        if type(val) != dict and i < len(nested_keys) - 1:
            return None
    return val


def nestedKeys(key):
    """ Transform string with nested keys into list.

    String should contain keys separated by dot. If a key contains
    dot itself, the key must be put between matching quotation marks.
    Quotation marks inside the keys (not preceding or following a dot)
    are treated as ordinary symbols. If a list is given instead of str,
    it is returned without changes.

    :param key: nested keys
    :type key: str, list

    :return: nested keys
    :rtype: list
    """
    if type(key) == list:
        return key
    nested_keys = []

    splitted_key = key.split('.')
    skip = []
    for i in range(len(splitted_key)):
        if i in skip:
            continue
        k = splitted_key[i]
        if k[0] in ("'", '"'):
            i1 = i
            while k[-1] not in ("'", '"'):
                i += 1
                if i1 >= len(splitted_key):
                    raise ValueError("Failed to decode dot-splitted "
                                     "configuration key: %s" % key)
                k1 = splitted_key[i1]
                k += k1
                skip += [i1]
        nested_keys.append(k)

    return nested_keys
