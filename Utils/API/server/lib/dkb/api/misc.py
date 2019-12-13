""" Miscellaneous functions. """


def sort_by_prefixes(values, prefixes, default=0):
    """ Classify values by 1-char prefixes.

    Generate hash with prefixes as a keys and lists of values -- as value:
    ```
    { prefix: [val1, val2, ...], ... }
    ```
    If there's no values for some prefix, empty list is set as its value.

    :raise TypeError:

    :param values: values to be sorted
    :type values: list
    :param prefixes: allowed 1-char prefixes
    :type profixes: list
    :param default: id of the prefix to be used ias default when none
                    specified. If set to None value without prefix will
                    produce a error.
    :type default: int, NoneType

    :return: hash with classified values
    :rtype: dict
    """
    result = {p: [] for p in prefixes}
    for v in values:
        try:
            if v[0] in result:
                result[v[0]] += [v[1:]]
            else:
                result[prefixes[default]] += [v]
        except IndexError:
            result[default] += ""
    if result.get(None):
        raise ValueError("Prefixes missed for: %r" % result[None])
    return result
