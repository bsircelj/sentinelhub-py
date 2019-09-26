from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from datetime import datetime

from .config import SHConfig


class Session:
    def __init__(self):
        self.cfg = SHConfig()
        client = BackendApplicationClient(client_id=self.cfg.oauth_client_id)
        self.oauth = OAuth2Session(client=client)
        self.token = None

    def __del__(self):
        self.oauth.close()

    def fetch_token(self):
        if self.token and self.token['expires_at'] > datetime.now().timestamp():
            return

        self.token = self.oauth.fetch_token(
            token_url=self.cfg.oauth_service_url,
            client_id=self.cfg.oauth_client_id,
            client_secret=self.cfg.oauth_secret
        )

    def post(self, content):
        self.fetch_token()
        return self.oauth.post(self.cfg.sentinel_processing_url, json=content)

    def get(self):
        self.fetch_token()
        return self.oauth.get(self.cfg.sentinel_processing_url)
