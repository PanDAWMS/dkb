"""
Extended CDSInvenioConnector allowing us to login via Kerberos
"""

__all__ = ["CDSInvenioConnector", "KerberizedCDSInvenioConnector"]

from invenio_client.contrib import cds
import sys

import splinter
try:
    import kerberos
except ImportError:
    pass

class CDSInvenioConnector(cds.CDSInvenioConnector):
    """ CDSInvenioConnector which closes the browser in most cases. """
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.browser:
            self.browser.quit()
        if isinstance(exc_val, KeyboardInterrupt):
            return True
        return False

class KerberizedCDSInvenioConnector(CDSInvenioConnector):
    """
    Represents same CDSInvenioConnector, but this one is aware about SPNEGO:
    Simple and Protected GSSAPI Negotiation Mechanism
    """
    def __init__(self, login="user", password="password"):
        """ Run parent's constructor with fake login/password

        ...to make it run _init_browser().
        Can't use input parameters as if they're empty strings, _init_browser
        won't be called.
        """
        try:
            kerberos
        except NameError:
            sys.stderr.write("ERROR: Seems like Kerberos Python package is not"
                             " installed. Can't proceed with Kerberos"
                             " authorization.\n")
            sys.exit(4)

        super(KerberizedCDSInvenioConnector, self).__init__("user", "password")

    def _init_browser(self):
        """
        Update it every time the CERN SSO login form is refactored.
        """
        try:
            (_, vc) = kerberos.authGSSClientInit("HTTP@login.cern.ch")
            kerberos.authGSSClientStep(vc, "")
            token = kerberos.authGSSClientResponse(vc)

            headers = {'Authorization': 'Negotiate '+token}

            self.browser = splinter.Browser('phantomjs', custom_headers=headers)
            self.browser.visit(self.server_url)
            self.browser.find_link_by_partial_text("Sign in").click()

        except kerberos.GSSError, e:
            sys.stderr.write(str(e)+"\n")
            sys.exit(3)

