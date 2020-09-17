"""
Module responsible for interaction with DKB storages.
"""

import es


class DKBStorageMethod(object):
    """ Wrapper class for all storage methods. """

    def __init__(self, name, storage):
        self.name = name
        try:
            self.action = getattr(storage, name)
        except AttributeError:
            # Not defined
            pass

    def __call__(self, **kwargs):
        return self.action(**kwargs)

    def action(self, **kwargs):
        """ Method action placeholder. """
        raise NotImplementedError("Method '%(name)s' is not implemented yet."
                                  % {'name': self.name})


# Hash of public method names and storage submodules in which to look for them
methods = {
    'task_steps_hist': es,
    'task_chain': es,
    'task_kwsearch': es,
    'task_derivation_statistics': es,
    'campaign_stat': es,
    'step_stat': es
}


# Define callable objects for public methods
for m in methods:
    globals()[m] = DKBStorageMethod(m, methods[m])
