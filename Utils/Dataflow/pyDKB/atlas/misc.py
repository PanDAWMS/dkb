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
