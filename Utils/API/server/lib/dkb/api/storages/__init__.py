"""
Module responsible for interaction with DKB storages.
"""

import es


def task_steps_hist(**kwargs):
    """ Get data for histogram of task distribution over time.

    Result hash is of following format:

    ```
    {
       "hist_data": { x1: [y1, y2, y3, ...], ...},
       "legend": ["y1_name", "y2_name", ...]
    }
    ```

    :return: hash of data for the histogram;
             False in case of ES client failure.
    :rtype: dict, bool
    """
    return es.task_steps_hist(**kwargs)
