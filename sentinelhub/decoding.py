"""
Module for data decoding
"""
import json
import struct
import tarfile
import warnings
from io import BytesIO
from xml.etree import ElementTree

import requests
import numpy as np
import tifffile as tiff
from PIL import Image

from .constants import MimeType
from .exceptions import ImageDecodingError


warnings.simplefilter('ignore', Image.DecompressionBombWarning)


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
    if data_type is MimeType.JSON:
        if isinstance(entire_response, requests.Response):
            return entire_response.json()
        return json.loads(response_content.decode('utf-8'))
    if data_type is MimeType.TAR:
        return decode_tar(response_content)
    if MimeType.is_image_format(data_type):
        return decode_image(response_content, data_type)
    if data_type is MimeType.XML or data_type is MimeType.GML or data_type is MimeType.SAFE:
        return ElementTree.fromstring(response_content)

    try:
        return {
            MimeType.TAR: decode_tar,
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


def decode_tar(data):
    """ A decoder to convert response bytes into a (image, metadata) tuple  # TODO: tuple with multiple values

    :param data: Data to decode
    :type data: bytes
    :return: A decoded image as numpy array and a metadata, which is a dict if successfully parsed, else None.
    :rtype: (np.ndarray, dict) or (np.ndarray, None)
    """
    tar = tarfile.open(fileobj=BytesIO(data))
    itr = ((member.name, get_data_format(member.name), tar.extractfile(member)) for member in tar.getmembers())
    dct = {fname: decode_data(file.read(), ftype) for fname, ftype, file in itr}
    return dct

def decode_sentinelhub_error_message(response):
    """ Decodes error message from Sentinel Hub service

    :param response: Sentinel Hub service response
    :type response: TODO
    :return: An error message
    :rtype: str
    """  # TODO: test if sometimes response is in json format
    try:
        server_message = []
        for elem in decode_data(response.content, MimeType.XML):
            if 'ServiceException' in elem.tag or 'Message' in elem.tag:
                server_message.append(elem.text.strip('\n\t '))
        return ''.join(server_message)
    except ElementTree.ParseError:
        return response.text


def get_jp2_bit_depth(stream):
    """ Reads bit encoding depth of jpeg2000 file in binary stream format

    :param stream: binary stream format
    :type stream: Binary I/O (e.g. io.BytesIO, io.BufferedReader, ...)
    :return: bit depth
    :rtype: int
    """
    stream.seek(0)
    while True:
        read_buffer = stream.read(8)
        if len(read_buffer) < 8:
            raise ValueError('Image Header Box not found in Jpeg2000 file')

        _, box_id = struct.unpack('>I4s', read_buffer)

        if box_id == b'ihdr':
            read_buffer = stream.read(14)
            params = struct.unpack('>IIHBBBB', read_buffer)
            return (params[3] & 0x7f) + 1


def fix_jp2_image(image, bit_depth):
    """ Because Pillow library incorrectly reads JPEG 2000 images with 15-bit encoding this function corrects the
    values in image.

    :param image: image read by opencv library
    :type image: numpy array
    :param bit_depth: bit depth of jp2 image encoding
    :type bit_depth: int
    :return: corrected image
    :rtype: numpy array
    """
    if bit_depth in [8, 16]:
        return image
    if bit_depth == 15:
        try:
            return image >> 1
        except TypeError:
            raise IOError('Failed to read JPEG 2000 image correctly. Most likely reason is that Pillow did not '
                          'install OpenJPEG library correctly. Try reinstalling Pillow from a wheel')

    raise ValueError('Bit depth {} of jp2 image is currently not supported. '
                     'Please raise an issue on package Github page'.format(bit_depth))


def get_data_format(filename):
    """ Util function to guess format from filename extension

    :param filename: name of file
    :type filename: str
    :return: file extension
    :rtype: MimeType
    """
    fmt_ext = filename.split('.')[-1]
    return MimeType(MimeType.canonical_extension(fmt_ext))
