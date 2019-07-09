"""
Extended CDSInvenioConnector allowing us to login via Kerberos
"""

import sys
import signal
import os

from pyDKB.common.misc import (log, logLevel)

__all__ = ["CDSInvenioConnector", "KerberizedCDSInvenioConnector"]


try:
    import kerberos
except ImportError:
    pass

try:
    from invenio_client.contrib import cds
    import splinter
except ImportError, e:
    log("Submodule failed (%s)" % e, logLevel.WARN)
    __all__ = []
else:

    class CDSInvenioConnector(cds.CDSInvenioConnector):
        """ CDSInvenioConnector which closes the browser in most cases. """

        orig_handlers = {}
        handlers = False

        def __init__(self, *args):
            self.orig_handlers = {
                signal.SIGINT: signal.signal(signal.SIGINT, self.kill),
                signal.SIGTERM: signal.signal(signal.SIGTERM, self.kill)
            }
            handlers = True
            super(CDSInvenioConnector, self).__init__(*args)

        def __enter__(self):
            """ Enter the with...as construction. """
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            """ Propagate exceptions after with...as construction. """
            self.delete()
            if exc_type:
                return False
            return True

        def __del__(self):
            self.delete(False)

        def delete(self, restore_handlers=True):
            if getattr(self, 'browser', None):
                self.browser.driver.service.process.send_signal(signal.SIGTERM)
                self.browser.quit()
                del self.browser
            if restore_handlers:
                for s in self.orig_handlers:
                    signal.signal(s, self.orig_handlers[s])

        def kill(self, signum, frame):
            """ Run del and propagate signal. """
            self.delete()
            os.kill(os.getpid(), signum)

    class KerberizedCDSInvenioConnector(CDSInvenioConnector):
        """
        Represents same CDSInvenioConnector, but this one is aware about
        SPNEGO: Simple and Protected GSSAPI Negotiation Mechanism
        """
        def __init__(self, login="user", password="password"):
            """ Run parent's constructor with fake login/password

            ...to make it run _init_browser().
            Can't use input parameters as if they're empty strings,
            _init_browser won't be called.
            """
            try:
                kerberos
            except NameError:
                log("Kerberos Python package is not"
                    " installed. Can't proceed with Kerberos"
                    " authorization.", logLevel.ERROR)
                sys.exit(4)

            super(KerberizedCDSInvenioConnector, self).__init__("user",
                                                                "password")

        def _init_browser(self):
            """
            Update it every time the CERN SSO login form is refactored.
            """
            try:
                (_, vc) = kerberos.authGSSClientInit("HTTP@login.cern.ch")
                kerberos.authGSSClientStep(vc, "")
                token = kerberos.authGSSClientResponse(vc)

                headers = {'Authorization': 'Negotiate ' + token}

                self.browser = splinter.Browser('phantomjs',
                                                custom_headers=headers)
                self.browser.visit(self.server_url)
                self.browser.find_link_by_partial_text("Sign in").click()

            except kerberos.GSSError, e:
                log("%s" % str(e), logLevel.ERROR)
                sys.exit(3)
