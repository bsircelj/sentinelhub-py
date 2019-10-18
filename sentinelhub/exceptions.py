"""
Module defining custom package exceptions
"""

class BaseSentinelHubException(Exception):
    """ A base package exception
    """


class DownloadFailedException(BaseSentinelHubException):
    """ General exception which is raised whenever download fails
    """


class OutOfRequestsException(DownloadFailedException):
    """ An exception raised whenever download cannot be done because user has run out of requests or processing units
    """


class AwsDownloadFailedException(DownloadFailedException):
    """ This exception is raised when download fails because of a missing file in AWS
    """


class ImageDecodingError(BaseSentinelHubException):
    """ This exception is raised when downloaded image is not properly decoded
    """