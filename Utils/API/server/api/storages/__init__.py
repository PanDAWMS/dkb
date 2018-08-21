"""
Module responsible for interaction with DKB storages.
"""

import es


def task_chain(tid):
    """ Get task IDs belonging to the same chain as passed ``tid``.

    :param tid: task ID
    :type tid: int, str

    :return: list of TaskIDs (empty if task with ``tid`` was not found);
             False in case of ES client failure.
    :rtype: list, bool
    """
    return es.task_chain(tid)
