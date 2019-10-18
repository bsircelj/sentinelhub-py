"""
Module for downloading data
"""

import logging
import os
import time
import warnings
import concurrent.futures

import requests
import boto3
from botocore.exceptions import NoCredentialsError

from .constants import MimeType, RequestType
from .config import SHConfig
from .os_utils import create_parent_folder, sys_is_windows
from .sentinelhub_session import SentinelHubSession
from .decoding import decode_data, decode_sentinelhub_error_message
from .exceptions import DownloadFailedException, AwsDownloadFailedException, ImageDecodingError


LOGGER = logging.getLogger(__name__)


class DownloadRequest:
    """ Class to manage HTTP requests

    Container for all download requests issued by the DataRequests containing
    url to Sentinel Hub's services or other sources to data, file names to
    saved data and other necessary flags needed when the data is downloaded and
    interpreted.

    :param url: url to Sentinel Hub's services or other sources from where the data is downloaded. Default is `None`
    :type url: str
    :param data_folder: folder name where the fetched data will be (or already is) saved. Default is `None`
    :type data_folder: str
    :param filename: filename of the file where the fetched data will be (or already is) saved. Default is `None`
    :type filename: str
    :param headers: add HTTP headers to request. Default is `None`
    :type headers: dict
    :param request_type: type of request, either GET or POST. Default is ``constants.RequestType.GET``
    :type request_type: constants.RequestType
    :param post_values: form encoded data to send in POST request. Default is `None`
    :type post_values: dict
    :param save_response: flag to turn on/off saving data downloaded by this request to disk. Default is `True`.
    :type save_response: bool
    :param return_data: flag to return or not data downloaded by this request to disk. Default is `True`.
    :type return_data: bool
    :param data_type: expected file format of downloaded data. Default is ``constants.MimeType.RAW``
    :type data_type: constants.MimeType
    """

    GLOBAL_AWS_CLIENT = None

    def __init__(self, *, url=None, data_folder=None, filename=None, headers=None, request_type=RequestType.GET,
                 post_values=None, save_response=True, return_data=True, data_type=MimeType.RAW, redownload=False,
                 session=None, **properties):

        self.url = url
        self.data_folder = data_folder
        self.filename = filename
        self.headers = headers
        self.post_values = post_values
        self.save_response = save_response
        self.return_data = return_data
        self.redownload = redownload
        self.session = session

        self.properties = properties

        self.request_type = RequestType(request_type)
        self.data_type = MimeType(data_type)

        self.s3_client = None
        self.file_path = None
        self._set_file_path()

    def set_filename(self, filename):
        """ Set filename attribute

        :param filename: Name of the file where .
        :return: str
        """
        self.filename = filename
        self._set_file_path()

    def set_data_folder(self, data_folder):
        """ Set data_folder attribute

        :param data_folder: folder name where the fetched data will be (or already is) saved.
        :return: str
        """
        self.data_folder = data_folder
        self._set_file_path()

    def _set_file_path(self):
        if self.data_folder and self.filename:
            self.file_path = os.path.join(self.data_folder, self.filename.lstrip('/'))
        elif self.filename:
            self.file_path = self.filename
        else:
            self.file_path = None
        if self.file_path and len(self.file_path) > 255 and sys_is_windows():
            warnings.warn('File path {} is longer than 255 character which might cause an error while saving on '
                          'disk'.format(self.file_path))
        elif self.file_path and len(self.filename) > 255:
            warnings.warn('Filename {} is longer than 255 character which might cause an error while saving on '
                          'disk'.format(self.filename))

    def get_file_path(self):
        """ Returns the full filename.

        :return: full filename (data folder + filename)
        :rtype: str
        """
        return self.file_path

    def set_save_response(self, save_response):
        """ Set save_response attribute

        :param save_response: flag to turn on/off saving data downloaded by this request to disk. Default is `True`.
        :return: bool
        """
        self.save_response = save_response

    def set_return_data(self, return_data):
        """ Set return_data attribute

        :param return_data: flag to return or not data downloaded by this request to disk. Default is `True`.
        :return: bool
        """
        self.return_data = return_data

    def is_download_required(self):
        """ Checks if it should even download
        """
        return (self.save_response or self.return_data) and (self.redownload or not self.is_downloaded())

    def is_downloaded(self):
        """ Checks if data for this request has already been downloaded and is saved to disk.

        :return: returns `True` if data for this request has already been downloaded and is saved to disk.
        :rtype: bool
        """
        if self.file_path is None:
            return False
        return os.path.exists(self.file_path)

    def is_aws_s3(self):
        """ Checks if data has to be downloaded from AWS s3 bucket

        :return: `True` if url describes location at AWS s3 bucket and `False` otherwise
        :rtype: bool
        """
        return self.url.startswith('s3://')


def download_data(request_list, redownload=False, max_threads=None, raise_download_errors=False):
    """ Download all requested data or read data from disk, if already downloaded and available and redownload is
    not required.

    :param request_list: list of DownloadRequests
    :type request_list: list of DownloadRequests
    :param redownload: if `True`, download again the data, although it was already downloaded and is available
                        on the disk. Default is `False`.
    :type redownload: bool
    :param max_threads: number of threads to use when downloading data; default is ``max_threads=None`` which
            by default uses the number of processors on the system multiplied by 5.
    :type max_threads: int
    :return: list of Futures holding downloaded data, where each element in the list corresponds to an element
                in the download request list.
    :rtype: list[concurrent.futures.Future]
    """
    for request in request_list: # TODO
        request.redownload = redownload

    LOGGER.debug("Using max_threads=%s for %s requests", max_threads, len(request_list))

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        download_list = [executor.submit(execute_download_request, request) for request in request_list]

    data_list = []
    for future in download_list:
        try:
            data_list.append(future.result(timeout=SHConfig().download_timeout_seconds))
        except ImageDecodingError as err:
            data_list.append(None)
            LOGGER.debug('%s while downloading data; will try to load it from disk if it was saved', err)
        except DownloadFailedException as download_exception:
            if raise_download_errors:
                raise download_exception
            warnings.warn(str(download_exception))
            data_list.append(None)
    return data_list


def execute_download_request(request):
    """ Executes download request.

    :param request: DownloadRequest to be executed
    :type request: DownloadRequest
    :return: downloaded data or None
    :rtype: numpy array, other possible data type or None
    :raises: DownloadFailedException
    """
    if request.save_response and request.data_folder is None:
        raise ValueError('Data folder is not specified. '
                         'Please give a data folder name in the initialization of your request.')

    if not request.is_download_required():
        return None

    try_num = SHConfig().max_download_attempts
    response = None
    while try_num > 0:
        try:
            if request.is_aws_s3():
                response = _do_aws_request(request)
                response_content = response['Body'].read()
            elif request.session:

                session = request.session
                response = session.post(json=request.post_values, headers=request.headers)

                response.raise_for_status()
                response_content = response.content

            else:
                response = _do_request(request)
                response.raise_for_status()
                response_content = response.content
            LOGGER.debug('Successful download from %s', request.url)
            break
        except requests.RequestException as exception:
            try_num -= 1
            if try_num > 0 and (_is_temporal_problem(exception) or
                                (isinstance(exception, requests.HTTPError) and
                                 exception.response.status_code >= requests.status_codes.codes.INTERNAL_SERVER_ERROR) or
                                _request_limit_reached(exception)):
                LOGGER.debug('Download attempt failed: %s\n%d attempts left, will retry in %ds', exception,
                             try_num, SHConfig().download_sleep_time)
                sleep_time = SHConfig().download_sleep_time
                if _request_limit_reached(exception):
                    sleep_time = max(sleep_time, 60)
                time.sleep(sleep_time)
            else:
                if request.url.startswith(SHConfig().aws_metadata_url) and \
                        isinstance(exception, requests.HTTPError) and \
                        exception.response.status_code == requests.status_codes.codes.NOT_FOUND:
                    raise AwsDownloadFailedException('File in location %s is missing' % request.url)
                raise DownloadFailedException(_create_download_failed_message(exception, request.url))

    _save_if_needed(request, response_content)

    if request.return_data:
        return decode_data(response_content, request.data_type, entire_response=response)
    return None


def _do_request(request):
    """ Executes download request
    :param request: A request
    :type request: DownloadRequest
    :return: Response of the request
    :rtype: requests.Response
    """
    if request.request_type is RequestType.GET:
        return requests.get(request.url, headers=request.headers)
    if request.request_type is RequestType.POST:
        return requests.post(request.url, json=request.post_values, headers=request.headers)
    raise ValueError('Invalid request type {}'.format(request.request_type))


def _do_aws_request(request):
    """ Executes download request from AWS service
    :param request: A request
    :type request: DownloadRequest
    :return: Response of the request
    :rtype: dict
    :raises: AwsDownloadFailedException, ValueError
    """
    if SHConfig().aws_access_key_id and SHConfig().aws_secret_access_key:
        key_args = dict(aws_access_key_id=SHConfig().aws_access_key_id,
                        aws_secret_access_key=SHConfig().aws_secret_access_key)
    else:
        key_args = {}
    aws_service, _, bucket_name, url_key = request.url.split('/', 3)

    try:
        s3_client = boto3.Session().client(aws_service.strip(':'), **key_args)
        request.s3_client = s3_client  # Storing the client prevents warning about unclosed socket
        DownloadRequest.GLOBAL_AWS_CLIENT = s3_client
    except KeyError:  # Sometimes creation of client fails and we use the global client if it exists
        if DownloadRequest.GLOBAL_AWS_CLIENT is None:
            raise ValueError('Failed to create a client for download from AWS')
        s3_client = DownloadRequest.GLOBAL_AWS_CLIENT

    try:
        return s3_client.get_object(Bucket=bucket_name, Key=url_key, RequestPayer='requester')
    except NoCredentialsError:
        raise ValueError('The requested data is in Requester Pays AWS bucket. In order to download the data please set '
                         'your access key either in AWS credentials file or in sentinelhub config.json file using '
                         'command line:\n'
                         '$ sentinelhub.config --aws_access_key_id <your AWS key> --aws_secret_access_key '
                         '<your AWS secret key>')
    except s3_client.exceptions.NoSuchKey:
        raise AwsDownloadFailedException('File in location %s is missing' % request.url)
    except s3_client.exceptions.NoSuchBucket:
        raise ValueError('Aws bucket %s does not exist' % bucket_name)


def _is_temporal_problem(exception):
    """ Checks if the obtained exception is temporal and if download attempt should be repeated

    :param exception: Exception raised during download
    :type exception: Exception
    :return: `True` if exception is temporal and `False` otherwise
    :rtype: bool
    """
    try:
        return isinstance(exception, (requests.ConnectionError, requests.Timeout))
    except AttributeError:  # Earlier requests versions might not have requests.Timeout
        return isinstance(exception, requests.ConnectionError)


def _request_limit_reached(exception):
    """ Checks if exception was raised because of too many executed requests. (This is a temporal solution and
    will be changed in later package versions.)

    :param exception: Exception raised during download
    :type exception: Exception
    :return: `True` if exception is caused because too many requests were executed at once and `False` otherwise
    :rtype: bool
    """
    return isinstance(exception, requests.HTTPError) and \
        exception.response.status_code == requests.status_codes.codes.TOO_MANY_REQUESTS


def _create_download_failed_message(exception, url):
    """ Creates message describing why download has failed

    :param exception: Exception raised during download
    :type exception: Exception
    :param url: An URL from where download was attempted
    :type url: str
    :return: Error message
    :rtype: str
    """
    message = 'Failed to download from:\n{}\nwith {}:\n{}'.format(url, exception.__class__.__name__, exception)

    if _is_temporal_problem(exception):
        if isinstance(exception, requests.ConnectionError):
            message += '\nPlease check your internet connection and try again.'
        else:
            message += '\nThere might be a problem in connection or the server failed to process ' \
                       'your request. Please try again.'
    elif isinstance(exception, requests.HTTPError):
        server_message = decode_sentinelhub_error_message(exception.response)
        message += '\nServer response: "{}"'.format(server_message)

    return message


def _save_if_needed(request, response_content):
    """ Save data to disk, if requested by the user

    :param request: Download request
    :type request: DownloadRequest
    :param response_content: content of the download response
    :type response_content: bytes
    """
    if request.save_response:
        file_path = request.get_file_path()
        create_parent_folder(file_path)
        with open(file_path, 'wb') as file:
            file.write(response_content)
        LOGGER.debug('Saved data from %s to %s', request.url, file_path)


def get_json(url, post_values=None, headers=None):
    """ Download request as JSON data type

    :param url: url to Sentinel Hub's services or other sources from where the data is downloaded
    :type url: str
    :param post_values: form encoded data to send in POST request. Default is `None`
    :type post_values: dict
    :param headers: add HTTP headers to request. Default is `None`
    :type headers: dict
    :return: request response as JSON instance
    :rtype: JSON instance or None
    :raises: RunTimeError
    """

    json_headers = {} if headers is None else headers.copy()

    if post_values is None:
        request_type = RequestType.GET
    else:
        request_type = RequestType.POST
        json_headers = {**json_headers, **{'Content-Type': MimeType.get_string(MimeType.JSON)}}

    request = DownloadRequest(url=url, headers=json_headers, request_type=request_type, post_values=post_values,
                              save_response=False, return_data=True, data_type=MimeType.JSON)

    return execute_download_request(request)


def get_xml(url):
    """ Download request as XML data type

    :param url: url to Sentinel Hub's services or other sources from where the data is downloaded
    :type url: str
    :return: request response as XML instance
    :rtype: XML instance or None
    :raises: RunTimeError
    """
    request = DownloadRequest(url=url, request_type=RequestType.GET, save_response=False, return_data=True,
                              data_type=MimeType.XML)

    return execute_download_request(request)
