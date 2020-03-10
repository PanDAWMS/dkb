"""
Module responsible for interaction with DKB storages.
"""

import es


def task_steps_hist(**kwargs):
    """ Get data for histogram of task distribution over time.

    :return: hash of data for the histogram
    :rtype: dict
    """
    return es.task_steps_hist(**kwargs)


def task_chain(**kwargs):
    """ Reconstruct task chain form given task ID.

    :param tid: task ID
    :type tid: int, str

    :return: task chain data
    :rtype: dict
    """
    return es.task_chain(**kwargs)


def task_kwsearch(**kwargs):
    """ Search tasks and related datasets by keywords.

    .. note: For wildcard keywords only the ``taskname`` field is used.

    :param kw: list of (string) keywords
    :type kw: list
    :param analysis: if analysis tasks should be searched
    :type analysis: str, bool
    :param production: if production tasks should be searched
    :type production: str, bool
    :param size: number of documents in response
    :type size: str, int
    :param ds_size: max number of datasets returned for each task
    :type ds_size: str, int
    :param timeout: request execution timeout (sec)
    :type timeout: str, int

    :return: task and related datasets metadata
    :rtype: dict
    """
    return es.task_kwsearch(**kwargs)


def task_derivation_statistics(**kwargs):
    """ Calculate statistics of derivation efficiency.

    :param project: project name
    :type project: str
    :param amitag: amitag (or several)
    :type amitag: str or list

    :return: calculated statistics in format required by
             :py:func:`api.handlers.task_deriv`
    :rtype: dict
    """
    return es.task_derivation_statistics(**kwargs)


def campaign_stat(**kwargs):
    """ Calculate values for campaign progress overview.

    :param path: full path to the method
    :type path: str
    :param htag: hashtag to select campaign tasks
    :type htag: str, list
    :param events_src: source of data for 'output' events.
                       Possible values:
                       * 'ds'   -- number of events in output datasets;
                       * 'task' -- number of processed events of 'done'
                                   and 'finished' tasks;
                       * 'all'  -- provide all possible values as hash.
    :type events_src: str

    :return: calculated campaign statistics
    :rtype: dict
    """
    return es.campaign_stat(**kwargs)


def step_stat(**kwargs):
    """ Calculate statistics for tasks by execution steps.

    :param selection_params: hash of parameters defining task selections
                             (for details see
                              :py:func:`es.common.get_selection_query`)
    :type selection_params: dict
    :param step_type: step definition type: 'step', 'ctag_format'
                      (default: 'step')
    :type step_type: str

    :return: hash with calculated statistics for ``step/stat`` method
             (see :py:func:`api.handlers.step_stat`)
    :rtype: hash
    """
    return es.step_stat(**kwargs)
