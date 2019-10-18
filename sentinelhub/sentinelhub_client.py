''' SentinelHub Client
'''
import json
import hashlib
import logging
import os
import time
import concurrent

from threading import Lock, get_ident

from .sentinelhub_session import SentinelHubSession
from .sentinelhub_rate_limit import SentinelHubRateLimit

LOGGER = logging.getLogger(__name__)

def log(msg):
    LOGGER.debug('[%d] %s', get_ident(), msg)

def log_header(headers):
    keys_to_log = {
        SentinelHubRateLimit.REQUEST_RETRY_HEADER: 'req_retry',
        SentinelHubRateLimit.REQUEST_COUNT_HEADER: 'req_limit',
        SentinelHubRateLimit.UNITS_RETRY_HEADER: 'token_retry',
        SentinelHubRateLimit.UNITS_COUNT_HEADER: 'token_limit',
        SentinelHubRateLimit.VIOLATION_HEADER: 'violation'
    }
    filt_keys = (key for key in headers if key in keys_to_log)
    hdrs = ', '.join(['{}({})'.format(keys_to_log[key], headers[key]) for key in filt_keys])
    log('UPDATE: {}'.format(hdrs))

def cache_response(func):
    """ Response-caching decorator
    """
    def wrapper(self, request, **kwargs):
        if self.cache_dir is None:
            return func(self, request, **kwargs)

        request_hash = hashlib.md5(request.encode('utf-8')).hexdigest()
        cache_dir = os.path.join(self.cache_dir, request_hash)

        img_filename = os.path.join(cache_dir, 'image.tar')
        if os.path.exists(img_filename):
            with open(img_filename, mode="rb") as img_file:
                return img_file.read()

        response = func(self, request, **kwargs)

        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        with open(img_filename, mode="wb") as img_file:
            img_file.write(response.content)

        req_filename = os.path.join(cache_dir, 'request.json')
        with open(req_filename, mode="w") as request_file:
            request_file.write(request)

        return response

    return wrapper


class SentinelHubClient:
    """ A Processing API client
    """
    def __init__(self, session=None, cache_dir=None):
        if session is None:
            self.session = SentinelHubSession()
        elif isinstance(session, SentinelHubSession):
            self.session = session
        else:
            raise ValueError("Invalid session")

        self.rate_limit = SentinelHubRateLimit(self.session)
        self.cache_dir = cache_dir
        self.lock = Lock()

    def download_list(self, requests, headers=None, max_threads=5):
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [executor.submit(self.download, request, headers=headers) for request in requests]

            # failed = next(future for future in concurrent.futures.as_completed(futures) if future.exception())
            # for future in concurrent.futures.as_completed(futures):
            #     if future.exception():
            #         log('DOWNLOAD FAILED')
            #         executor._threads.clear()
            #         # executor.shutdown(wait=False)
            #         raise future.exception()

        return [future.result() for future in futures]
        # return [self.download(req, headers=headers) for req in requests]

    def download(self, request, headers=None):
        ''' Get the requested image
        '''
        while True:
            self.lock.acquire()
            wait_time = self.rate_limit.register_next()
            self.lock.release()

            if wait_time:
                log('SLEEP({})'.format(wait_time))
                time.sleep(wait_time)
                # continue

            log('START DOWNLOAD')
            response = self._execute_request(request=json.dumps(request), headers=headers)
            log_header(response.headers)

            self.lock.acquire()
            self.rate_limit.update(response.headers)
            self.lock.release()

            if response.status_code == 200:
                log('DONE')
                return response if isinstance(response, bytes) else response.content

    @cache_response
    def _execute_request(self, request, headers=None):
        ''' Fetch Sentinelhub data based on request argument as bytes so it can be hashed for caching
        '''

        response = self.session.post(data=request, headers=headers)

        if response.status_code not in [200, 429]:
            log('Exception! {}'.format(response.status_code))
            raise SentinelHubException(response)

        return response


class SentinelHubException(BaseException):
    """ Server's internal error exception
    """
    def __init__(self, response):
        response = json.loads(response.content)
        super().__init__(response['error']['message'])
