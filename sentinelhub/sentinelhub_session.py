"""
Module implementing Sentinel Hub session object
"""
import time
from datetime import datetime

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from .config import SHConfig
from .constants import OgcConstants
from .sentinelhub_rate_limit import SentinelHubRateLimit


class SentinelHubSession:
    """ Processing API session to handle authentification and rate-limiting
    """
    def __init__(self, config=None):
        self.config = SHConfig() if config is None else config
        client = BackendApplicationClient(client_id=self.config.sh_client_id)
        self.oauth = OAuth2Session(client=client)

        # TODO: It's not ok to keep OAuth2Session alive for the lifetime of self (check requests.api.request),
        #       because it keeps the socket open. Instead, just keep the token and instantiate a new session for
        #       each request.

        self._token = None

        self.count = 0
        self.rate_limit = SentinelHubRateLimit(self)

    def __del__(self):
        self.oauth.close()

    @property
    def token(self):
        """ Optionally updates and returns session's token
        """
        if self._token and self._token['expires_at'] > time.time() + 5:
            return self._token

        self._token = self._fetch_token()

        return self._token

    @property
    def session_headers(self):
        """ Provides
        """
        return {
            'Authorization': 'Bearer {}'.format(self.token['access_token']),
            **OgcConstants.HEADERS  # TODO: rename OgcConstants to SHConstants, maybe this could be moved somewhere else?
        }

    def _fetch_token(self):
        """ Collects new token
        """
        return self.oauth.fetch_token(
            token_url=self.config.get_sh_oauth_url(),
            client_id=self.config.sh_client_id,
            client_secret=self.config.sh_client_secret
        )

    def post(self, url=None, **kwargs):
        """ Execute a post request through the oauth session and handle rate-limiting
        """
        if url is None:
            url = self.config.get_sh_processing_api_url()

        _ = self.token # TODO

        return self.oauth.post(url, **kwargs)

    def get(self, url=None, **kwargs):

        if url is None:
            url = self.config.get_sh_processing_api_url()

        _ = self.token # TODO

        return self.oauth.get(url, **kwargs)
