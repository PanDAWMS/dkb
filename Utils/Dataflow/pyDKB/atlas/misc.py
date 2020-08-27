"""
Miscellaneous functions required to operate with ATLAS metadata objects.
"""


def dataset_data_format(datasetname):
    """ Extract data format from dataset name.

    According to dataset naming nomenclature:
    https://dune.bnl.gov/w/images/9/9e/Gen-int-2007-001_%28NOMENCLATURE%29.pdf
    for MC datasets:
        mcNN_subProject.datasetNumber.physicsShort.prodStep.dataType.Version
    for Real Data:
        DataNN_subProject.runNumber.streamName.prodStep.dataType.Version
    In both cases the dataType field is required.

    :param datasetname: dataset name
    :type datasetname: str

    :return: data format,
             None if `datasetname` is None, empty string, etc.
    :rtype: str
    """
    if not datasetname:
        return None
    splitted = datasetname.split('.')
    N = len(splitted)
    ds_format = None
    if N:
        project = splitted[0]
        if project in ('user', 'group'):
            if N > 7:
                ds_format = splitted[6]
        elif N > 5:
            ds_format = splitted[4]
    return ds_format


def dataset_scope(dsn):
    """ Extract the first field from the dataset name

    Example:
      mc15_13TeV.XXX
      mc15_13TeV:YYY.XXX

    :param dsn: full dataset name
    :type dsn: str

    :return: dataset scope
    :rtype: str
    """
    pos = dsn.find(':')
    if pos > -1:
        result = dsn[:pos]
    else:
        scope = dsn.split('.')[0]
        if dsn.startswith('user') or dsn.startswith('group'):
            scope = '.'.join(dsn.split('.')[0:2])
        result = scope
    return result


def normalize_dataset_name(dsn):
    """ Remove an explicitly stated scope from a dataset name.

    According to dataset nomenclature, dataset name cannot include
    a ':' symbol. If a dataset name is in 'A:B' format, then A,
    probably, is an explicitly stated scope that should be removed.

    :param dsn: dataset name
    :type dsn: str

    :return: dataset name without explicit scope,
             unchanged dataset name if it was already normal
    :rtype: str
    """
    pos = dsn.find(':')
    if pos > -1:
        result = dsn[(pos + 1):]
    else:
        result = dsn
    return result
