"""
Module implementing support for Sentinel Hub processing API
"""

import datetime as dt

from .constants import OgcConstants, MimeType
from .download import DownloadRequest
from .ogc import OgcImageService


class ApiImageService(OgcImageService):
    """
    TODO: don't inherit from Ogc
    """
    def __init__(self, **kwargs):

        super().__init__(**kwargs)

        self.api_url = 'https://services.sentinel-hub.com/api/v1/process'

    def get_request(self, request):
        """ Get download requests

        Create a list of DownloadRequests for all Sentinel-2 acquisitions within request's time interval and
        acceptable cloud coverage.

        :param request: QGC-type request with specified bounding box, time interval, and cloud coverage for specific
                        product.
        :type request: OgcRequest or GeopediaRequest
        :return: list of DownloadRequests
        """
        size_x, size_y = self.get_image_dimensions(request)
        # TODO: most of _get_payload could be calculated once, only small part depends on dates
        return [DownloadRequest(url=self.api_url,
                                post_values=self._get_payload(request=request, timestamp=timestamp,
                                                              size_x=size_x, size_y=size_y),
                                filename='test.png', # self.get_filename(request, timestamp, size_x, size_y),
                                data_type=request.image_format, headers=OgcConstants.HEADERS)  # TODO: some flag for oauth
                for timestamp in self.get_dates(request)]

    def _get_payload(self, request, timestamp, size_x, size_y):

        return {
            "input": {
                "bounds": self._get_bounds_payload(request),
                "data": self._get_data_payload(request, timestamp)
            },
            "evalscript": request.evalscript,  # TODO: parsing
            "output": self._get_output_payload(request, size_x, size_y)
        }

    @staticmethod
    def _get_bounds_payload(request):
        """ Creates the part of the payload about geographical location

        :param request:
        :return:
        """
        payload = {
            "properties": {
                "crs": request.bbox.crs.opengis_string
            }
        }

        if request.bbox:
            payload['bbox'] = list(request.bbox)

        if request.geometry:
            payload['geometry'] = None

        return payload

    @staticmethod
    def _get_data_payload(request, timestamp):

        return [
            {
                "type": 'S2L1C',
                "dataFilter": {
                    "timeRange": {
                        "from": timestamp.isoformat() + 'Z',
                        "to": (timestamp + dt.timedelta(seconds=1)).isoformat() + 'Z'
                    }
                }
            }
        ]

    @staticmethod
    def _get_output_payload(request, size_x, size_y):

        format_payload = {
            'type': request.image_format.get_string()
        }
        if request.image_format is MimeType.JPG:
            format_payload['quality'] = 100

        return {
                "width": size_x,
                "height": size_y,
                "responses": [  # TODO: multiple formats?
                    {
                        "identifier": "default",  # TODO: check this
                        "format": format_payload
                    }
                ]
            }