"""
Module responsible for interaction with DKB storages.
"""

import logging

import es
import es_nested


class DKBStorageMethod(object):
    """ Wrapper class for all storage methods. """

    def __init__(self, name, storage):
        self.name = name
        self.alts = {}
        try:
            self.action = getattr(storage, name)
        except AttributeError:
            # Not defined
            pass

    def __call__(self, **kwargs):
        """

        TODO: replase `_alt` parameter usage with direct calls of `use_alt`
              at the handlers' level.
        """
        alt = kwargs.pop('_alt', None)
        if alt:
            try:
                res = self.use_alt(alt, **kwargs)
            except NotImplementedError, e:
                logging.warn(e)

                # Try default implementation instead
                res = self.action(**kwargs)

                # ...and add warning to metadata
                data, metadata = res
                warn = "Default method implementation is used: %s" % str(e)
                if 'warning' not in metadata:
                    metadata['warning'] = warn
                elif type(metadata['warnin']) is list:
                    metadata['warning'].append(warn)
                else:
                    metadata['warning'] = [metadata['warning'], warn]
        else:
            res = self.action(**kwargs)
        return res

    def use_alt(self, alt, **kwargs):
        """ Try alternative implementation of current method.

        :raises: ``NotImplementedError`` (requested alternative is not defined)

        :param alt: alternative implementation that should be tried
        :type alt: str
        """
        try:
            alt_method = self.alts[alt]
        except (AttributeError, KeyError):
            raise NotImplementedError("Alternative '%s' implementation for"
                                      " method '%s' is not defined."
                                      % (alt, self.name))

        try:
            res = alt_method(**kwargs)
        except NotImplementedError:
            raise NotImplementedError("Alternative '%s' implementation for"
                                      " method '%s' is not implemented yet."
                                      % (alt, self.name))

        return res


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

    # Define alternative implementation for ES methods (based on nested storage
    # scheme)
    if methods[m] == es:
        globals()[m].alts['nested'] = DKBStorageMethod(m, es_nested)
