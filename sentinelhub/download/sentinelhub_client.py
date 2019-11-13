"""
Module implementing a rate-limited multi-threaded download client for downloading from Sentinel Hub service
"""
import logging
import time
from threading import Lock, get_ident

import requests

from .handlers import fail_user_errors, retry_temporal_errors
from .client import DownloadClient
from ..sentinelhub_session import SentinelHubSession
from ..sentinelhub_rate_limit import SentinelHubRateLimit


LOGGER = logging.getLogger(__name__)


class SentinelHubDownloadClient(DownloadClient):

    def __init__(self, *, session=None, **kwargs):

        super().__init__(**kwargs)

        if session is None:  # TODO: what if user doesn't configure client credentials and would just like to use OGC?
            self.session = SentinelHubSession(config=self.config)
        elif isinstance(session, SentinelHubSession):
            self.session = session
        else:
            raise ValueError('Invalid session')

        self.rate_limit = SentinelHubRateLimit(self.session)
        self.lock = Lock()

    @retry_temporal_errors
    @fail_user_errors
    def _execute_download(self, request):

        while True:
            self.lock.acquire()
            sleep_time = self.rate_limit.register_next()
            self.lock.release()

            if sleep_time == 0:
                # log('START DOWNLOAD')
                response = self._do_download(request)

                self.lock.acquire()
                # log(response.headers)
                # log(f'status code={response.status_code}')
                self.rate_limit.update(response.headers)
                self.lock.release()

                if response.status_code != requests.status_codes.codes.TOO_MANY_REQUESTS:
                    response.raise_for_status()

                    # log(f'Successful download from {request.url}')
                    # LOGGER.debug('Successful download from %s', request.url)
                    return response.content
            else:
                # log('SLEEP {}s'.format(sleep_time))
                time.sleep(sleep_time)

    def _do_download(self, request):

        headers = {
            **self.session.session_headers,
            **request.headers
        }
        return requests.request(request.request_type.value, url=request.url, json=request.post_values, headers=headers)


def log(msg):
    LOGGER.debug('[%d] %s', get_ident(), msg)

