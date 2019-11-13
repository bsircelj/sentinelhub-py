"""
Module implementing async download
"""

import asyncio
import functools
import logging
import time

import httpx

import aiohttp
from aiohttp import ClientSession
from aiohttp_oauth_client import OAuth2Client

import nest_asyncio

from .constants import MimeType, RequestType
from .config import SHConfig
from .download import SentinelHubRateLimit
from .io_utils import read_data, write_data
from .sentinelhub_session import SentinelhubSession
from .decoding import decode_data, decode_sentinelhub_error_message
from .exceptions import DownloadFailedException, AwsDownloadFailedException, ImageDecodingError



LOGGER = logging.getLogger(__name__)


class DownloadClient:

    def __init__(self, connections, redownload=False, raise_download_errors=True, reader=read_data,
                 writer=functools.partial(write_data, data_format=MimeType.RAW), sh_session=None, config=None):

        self.connections = connections
        self.redownload = redownload
        self.raise_download_errors = raise_download_errors

        self.reader = reader
        self.writer = writer

        self.sh_session = sh_session  # TODO
        self.config = SHConfig() if config is None else config

        self.rate_limit = SentinelHubRateLimit()

    def download_data(self, download_list):

        # TODO: divide requests according to different kind of downloads

        return self._async_download(download_list, sh_session=download_list[0].session)

    def _async_download(self, download_list, sh_session=None):

        event_loop = asyncio.get_event_loop()

        if event_loop.is_running():
            # In case the code is executed in Jupyter notebook there is already async event loop running (by Jupyter's
            # tornado server). By default asyncio doesn't allow nesting event loops. The following line fixes that.
            nest_asyncio.apply()

        results = event_loop.run_until_complete(self._async_session_download(event_loop, download_list,
                                                                             sh_session=sh_session))

        if not event_loop.is_running():
            event_loop.close()

        return results

    async def _async_session_download(self, event_loop, download_list, sh_session=None):
        connector = aiohttp.TCPConnector(limit=self.connections)
        if sh_session is None:
            client_session = ClientSession(loop=event_loop, connector=connector)
        else:
            client_session = OAuth2Client(client_id=self.config.client_id, client_secret=self.config.client_secret,
                                          connector=connector)  # token=Token # TODO
        # TODO: are you sure?
        async with client_session as session:

            return await self._execute_download(session, download_list)

    async def _execute_download(self, session, download_list):

        download_queue = asyncio.Queue(maxsize=2 * self.connections)  # TODO: think
        result_list = [None] * len(download_list)

        consumer_list = [asyncio.ensure_future(
            self._consume(download_queue, session, result_list)) for _ in range(self.connections)
        ]

        await self._produce(download_queue, download_list)

        await download_queue.join()

        for consumer_future in consumer_list:
            consumer_future.cancel()

        return result_list

    async def _consume(self, *args):
        """ The producer is ever trying to download the next request
        """
        while True:
            await self._execute_request(*args)

    @staticmethod
    async def _produce(download_queue, download_list):
        """ The producer function which puts requests into the queue
        """
        for index, request in enumerate(download_list):
            # If the queue is full(reached maxsize) this line will be blocked
            # until a consumer will finish processing a url
            await download_queue.put((index, request))

    async def _execute_request(self, download_queue, session, result_list):
        if not self.rate_limit.register_next():
            return

        # Whenever queue is empty the following will fail and consumer will finish
        index, request = await download_queue.get()
        # TODO: read about async queue - maybe you have to call task_done() here?

        try:
            async with session.request(request.request_type.value, request.url, headers=request.headers,
                                       json=request.post_values,  # TODO: check post
                                       timeout=self.config.download_timeout_seconds) as response:
                response_content = await response.content.read()

                # self.rate_limit.update(response.headers)

                response.raise_for_status()
            result_list[index] = self._process_content(response_content, request)

            download_queue.task_done()

        except BaseException as exception:
            print(exception)
            # Check if rate limiting
            pass
            if self._is_temporal_exception():
                pass
                # backoff logic

            # Prepare error message and return
            # print(err)
            raise err  # TODO
            # we put the url in the dlq, so other consumers wil handle it
            result_list[index] = result
            # lower the pace
            asyncio.sleep(5)
            # telling the queue we finished processing the massage

        # download_queue.task_done()

    def _process_content(self, response_content, request):
        """ Processes downloaded content according to parameters of DownloadRequest class instance
        """
        if request.save_response:
            file_path = request.get_file_path()

            self.writer(file_path, response_content)
            LOGGER.debug('Saved data from %s to %s', request.url, file_path)

        if request.return_data:
            return decode_data(response_content, request.data_type)
        return None
