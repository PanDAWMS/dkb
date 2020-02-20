"""
Module responsible for interaction with DKB storages.
"""

import es


def task_steps_hist(**kwargs):
    """ Get data for histogram of task distribution over time.

    Result hash is of following format:

    ```
    {
      'legend': ['series1_name', 'series2_name', ...],
      'data': {
        'x': [
          [x1_1, x1_2, ...],
          [x2_1, x2_2, ...],
          ...
        ],
        'y': [
          [y1_1, y1_2, ...],
          [y2_1, y2_2, ...],
          ...
        ]
      }
    }
    ```

    Series can be of different length, but ``xN`` and ``yN`` arrays
    have same length.

    :return: hash of data for the histogram
    :rtype: dict
    """
    return es.task_steps_hist(**kwargs)


def task_chain(**kwargs):
    """ Reconstruct task chain form given task ID.

    :param tid: task ID
    :type tid: int, str

    :return: task chain:
             {
                 ...,
                 taskidN: [childN1_id, childN2_id, ...],
                 ...
             }
    :rtype: dict
    """
    return es.task_chain(**kwargs)


def task_kwsearch(**kwargs):
    """ Search tasks and related datasets by keywords.

    For wildcard keywords only ``taskname`` field is used.

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

    :return: task and related datasets info:
             { _took_storage_ms: <storage query execution time in ms>,
               _total: <total number of matching tasks>,
               _data: [..., {..., output_dataset: [{...}, ...], ...}, ...],
               _errors: [..., <error message>, ...]
             }
             (field `_errors` may be omitted if no error has occured)
    :rtype: dict
    """
    return es.task_kwsearch(**kwargs)


def task_derivation_statistics(**kwargs):
    """ Calculate statistics of derivation efficiency.

    Resulting statistics has the following structure:
    {
      'data': [
        ...
        {
          'output': 'SOME_OUTPUT_FORMAT',
          'tasks': 123,
          'task_ids': [id1, id2, ...],
          'ratio': 0.456,
          'events_ratio': 0.789
        },
        ...
      ]
    }

    :param project: project name
    :type project: str
    :param amitag: amitag (or several)
    :type amitag: str or list

    :return: calculated statistics
    :rtype: dict
    """
    return es.task_derivation_statistics(**kwargs)


def campaign_stat(**kwargs):
    """ Calculate values for campaign progress overview.

    :param path: full path to the method
    :type path: str
    :param htag: hashtag to select campaign tasks
    :type htag: str, list

    :return: calculated campaign statistics:
             { _took_storage_ms: <storage query execution time in ms>,
               _total: <total number of matching tasks>,
               _errors: [..., <error message>, ...],
               _data: {
                 last_update: <last_registered_task_timestamp>,
                 date_format: <datetime_format>,
                 tasks_processing_summary: {
                   <step>: {
                     <status>: <n_tasks>, ...,
                     start: <earliest_start_time>,
                     end: <latest_end_time>
                   },
                   ...
                 },
                 overall_events_processing_summary: {
                   <step>: {
                     input: <n_events>,
                     output: <n_events>,
                     ratio: <output>/<input>
                   },
                   ...
                 },
                 tasks_updated_24h: {
                   <step>: {
                     <status>: {
                       total: <n_tasks>,
                       updated: <n_tasks>
                     },
                     ...
                   },
                   ...
                 },
                 events_24h: {
                   <step>: <n_output_events_for_done_finisfed>,
                   ...
                 }
               }
             }
             (field `_errors` may be omitted if no error has occured)
    :rtype: dict
    """
    return es.campaign_stat(**kwargs)
