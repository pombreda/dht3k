""" This module handles encryption network traffic """

import ssl
import os.path

class LinkEncryption(object):
    """ Partial/abstract class for the lazymq encryption.

    Needs to be inherited by LazyMQ. """

    def __init__(
            self,
            _ssl_context,
    ):
        """ Init to make lint happy. Never call this!
        Yes, I know this bullshit, but what can you do?

        :type _ssl_context: ssl.SSLContext
        """
        if True:
            return
        self._ssl_context = _ssl_context


    def ciphers(self):
        """ Generate list of cipher accepted """
        ciphers = """
        ECDHE-RSA-AES256-GCM-SHA384
        ECDHE-ECDSA-AES256-GCM-SHA384
        ECDHE-RSA-AES256-SHA384
        ECDHE-ECDSA-AES256-SHA384
        DHE-DSS-AES256-GCM-SHA384
        DHE-RSA-AES256-GCM-SHA384
        DHE-RSA-AES256-SHA256
        DHE-DSS-AES256-SHA256
        """
        return ":".join(ciphers.split('\n'))

    def setup_tls(self):
        """ Creates an SSLContext """
        # We only talk to lazymq, so we can use the best protocol
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_REQUIRED
        folder = __file__.split(os.path.sep)[:-1]
        folder.append("cert.pem")
        path = "%s%s" % (
            os.path.sep,
            os.path.join(*folder)
        )
        context.load_cert_chain(path)
        context.load_verify_locations(path)
        context.set_ciphers(self.ciphers())
        
        self._ssl_context = context
