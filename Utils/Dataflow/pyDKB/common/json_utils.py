"""
Utils to work with JSON (dict) objects.
"""

def valueByKey(json_data, key):
    """ Return value by a chain (list) of nested keys.

    Parameters:
        DICT   json_data -- to search in
        STRING key       -- dot-separated list of nested keys
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
                values.append(valueByKey(val[j], nested_keys[i+1:]))
            return values
        if type(val) != dict and i < len(nested_keys)-1:
            return None
    return val

def nestedKeys(key):
    """ Transform STRING with nested keys into LIST.

    Parameters:
        STRING key -- dot-separated list of nested keys.
                      If a key contains dot itself, the key must be put between
                      quotation marks.
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
