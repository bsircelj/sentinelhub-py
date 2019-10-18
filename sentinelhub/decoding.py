"""
Module for data decoding
"""
import tarfile
import logging
import json
import warnings
from io import BytesIO
from xml.etree import ElementTree

import requests
import numpy as np
import tifffile as tiff
from PIL import Image

from .constants import MimeType
from .exceptions import ImageDecodingError
from .io_utils import get_jp2_bit_depth, fix_jp2_image


warnings.simplefilter('ignore', Image.DecompressionBombWarning)
LOGGER = logging.getLogger(__name__)


def decode_data(response_content, data_type, entire_response=None):  # TODO: resolve entire_response
    """ Interprets downloaded data and returns it.

    :param response_content: downloaded data (i.e. json, png, tiff, xml, zip, ... file)
    :type response_content: bytes
    :param data_type: expected downloaded data type
    :type data_type: constants.MimeType
    :param entire_response: A response obtained from execution of download request
    :type entire_response: requests.Response or dict or None
    :return: downloaded data
    :rtype: numpy array in case of image data type, or other possible data type
    :raises: ValueError
    """
    LOGGER.debug('data_type=%s', data_type)

    if data_type is MimeType.JSON:
        if isinstance(entire_response, requests.Response):
            return entire_response.json()
        return json.loads(response_content.decode('utf-8'))
    if MimeType.is_image_format(data_type):
        return decode_image(response_content, data_type)
    if data_type is MimeType.XML or data_type is MimeType.GML or data_type is MimeType.SAFE:
        return ElementTree.fromstring(response_content)

    try:
        return {
            MimeType.RAW: response_content,
            MimeType.TXT: response_content,
            MimeType.REQUESTS_RESPONSE: entire_response,
            MimeType.ZIP: BytesIO(response_content)
        }[data_type]
    except KeyError:
        raise ValueError('Unknown response data type {}'.format(data_type))


def decode_image(data, image_type):
    """ Decodes the image provided in various formats, i.e. png, 16-bit float tiff, 32-bit float tiff, jp2
    and returns it as an numpy array

    :param data: image in its original format
    :type data: any of possible image types
    :param image_type: expected image format
    :type image_type: constants.MimeType
    :return: image as numpy array
    :rtype: numpy array
    :raises: ImageDecodingError
    """
    bytes_data = BytesIO(data)
    if image_type.is_tiff_format():
        image = tiff.imread(bytes_data)
    else:
        image = np.array(Image.open(bytes_data))

        if image_type is MimeType.JP2:
            try:
                bit_depth = get_jp2_bit_depth(bytes_data)
                image = fix_jp2_image(image, bit_depth)
            except ValueError:
                pass

    if image is None:
        raise ImageDecodingError('Unable to decode image')
    return image


def decode_sentinelhub_error_message(response):
    """ Decodes error message from Sentinel Hub service

    :param response: Sentinel Hub service response
    :type response: TODO
    :return: An error message
    :rtype: str
    """
    try:
        server_message = []
        for elem in decode_data(response.content, MimeType.XML):
            if 'ServiceException' in elem.tag or 'Message' in elem.tag:
                server_message.append(elem.text.strip('\n\t '))
        return ''.join(server_message)
    except ElementTree.ParseError:
        return response.text

def decode_tar(data):
    ''' A decoder to convert response bytes into a (image, metadata) tuple

    :param data: Data to decode.
    :type data: bytes
    :return: A decoded image as numpy array and a metadata, which is a dict if successfuly parsed, else None.
    :rtype: (np.ndarray, dict) or (np.ndarray, None)
    '''
    tar = tarfile.open(fileobj=BytesIO(data))

    tif_member = next(member for member in tar.getmembers() if member.name.endswith('.tif'))
    img_file = tar.extractfile(tif_member)
    image = decode_image(img_file.read(), MimeType.TIFF_d16)

    json_member = next(member for member in tar.getmembers() if member.name.endswith('.json'))
    json_file = tar.extractfile(json_member)
    metadata = decode_data(json_file.read(), MimeType.JSON)

    return image, metadata
