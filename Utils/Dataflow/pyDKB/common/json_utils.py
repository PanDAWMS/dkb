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
        if isinstance(val, list):
            values = []
            for j in range(len(val)):
                values.append(valueByKey(val[j], nested_keys[i + 1:]))
            return values
        if not isinstance(val, dict) and i < len(nested_keys) - 1:
            return None
    return val


def nestedKeys(key):
    """ Transform STRING with nested keys into LIST.

    Parameters:
        STRING key -- dot-separated list of nested keys.
                      If a key contains dot itself, the key must be put between
                      quotation marks.
    """
    if isinstance(key, list):
        return key

    # Resulting list of keys
    nested_keys = []
    # Temporary string for constructing keys.
    new_key = ''
    # Current state of the cycle regarding quotes.
    # Empty string - normal mode, keys are separated by dots.
    # ' - a key limited by single quotes is being constructed.
    # " - same, but with double quotes.
    mode = ''

    for i in range(len(key)):
        if mode:
            if key[i] == mode and (i == len(key) - 1 or key[i + 1] == '.'):
                mode = ''
            elif i == len(key) - 1:
                raise ValueError("Failed to decode dot-splitted "
                                 "configuration key: %s" % key)
            else:
                new_key += key[i]
        else:
            if key[i] == '.':
                nested_keys.append(new_key)
                new_key = ''
            elif key[i] in ("'", '"') and new_key == '':
                # The second condition checks that the quote is preceded
                # either by dot or string start.
                mode = key[i]
            else:
                new_key += key[i]
    nested_keys.append(new_key)

    return nested_keys
