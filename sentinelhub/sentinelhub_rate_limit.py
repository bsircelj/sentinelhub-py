"""
Module implementing rate limiting logic for Sentinel Hub service
"""

import time
from enum import Enum
import logging
from threading import get_ident
import requests
import jwt

# from .download import get_json
from .exceptions import OutOfRequestsException


class SentinelHubRateLimit:

    REQUEST_RETRY_HEADER = 'Retry-After'
    REQUEST_COUNT_HEADER = 'X-RateLimit-Remaining'
    UNITS_RETRY_HEADER = 'X-ProcessingUnits-Retry-After'
    UNITS_COUNT_HEADER = 'X-ProcessingUnits-Remaining'
    VIOLATION_HEADER = 'X-RateLimit-ViolatedPolicy'

    def __init__(self, session, *, status_refresh_period=120):

        self.session = session
        self.status_refresh_period = status_refresh_period

        self.requests_in_process = 0  # TODO: integrate this

        self.expected_requests_per_second = None  # TODO: number of parallel processors
        self.expected_cost_per_request = None

        self.last_status_update_time = None

        self.policy_buckets = self._initialize_policy_buckets()
        # print(self.policy_buckets)

    def _initialize_policy_buckets(self):
        user_policies = self._fetch_user_policies()
        # print(user_policies)
        buckets = []
        for policy_payload in user_policies['data']:

            policies = policy_payload['policies']
            if not policies:
                policies = policy_payload['type']['defaultPolicies']

            policy_type = PolicyType(policy_payload['type']['name'])

            for policy in policies:
                buckets.append(PolicyBucket(policy_type, policy))

        return buckets

    def _fetch_user_policies(self):
        """ Collects user rate limiting policies
        """
        # TODO: change after they update backend
        user_info = jwt.decode(self.session.token['access_token'], verify=False)
        user_id = user_info['sub']

        url = '{}/contract?userId=eq:{}'.format(self.session.config.get_sh_rate_limit_url(), user_id)

        return requests.get(url, headers=self.session.session_headers).json()
        # return get_json(url, headers=self.session.session_headers)

    def _fetch_status(self):
        """ Collects status about remaining requests and processing units
        """
        url = '{}/statistics/tokenCounts'.format(self.session.config.get_sh_rate_limit_url())

        return requests.get(url, headers=self.session.session_headers).json()
        # return get_json(url, headers=self.session.session_headers)

    def register_next(self):
        """ Determines if next download request can start or not
        """
        if self.last_status_update_time is None or \
                self.last_status_update_time + self.status_refresh_period <= time.time():
            self._update_status()

        elapsed_time = time.time() - self.last_status_update_time
        # print(self.expected_requests_per_second, self.expected_cost_per_request, self.requests_in_process)
        # print(self.last_status_update_time, elapsed_time)
        max_wait_time = 0
        for bucket in self.policy_buckets:

            expected_cost_per_request = 1 if bucket.is_request_bucket() else self.expected_cost_per_request
            expected_wait_time = bucket.get_expected_wait_time(elapsed_time,
                                                               self.expected_requests_per_second,
                                                               expected_cost_per_request, self.requests_in_process)
            if expected_wait_time == -1:
                # TODO: better logic
                raise OutOfRequestsException

            max_wait_time = max(max_wait_time, expected_wait_time)

        if max_wait_time == 0:
            self.requests_in_process += 1
        # TODO: it should remember allowed request
        return max_wait_time

    def _update_status(self):
        new_status = self._fetch_status()
        new_status_update_time = time.time()
        # print(new_status)
        new_content_list = [new_status['data'][bucket.policy_type.value][bucket.refill_period]
                            for bucket in self.policy_buckets]

        if self.last_status_update_time is None:
            self.expected_requests_per_second = min(bucket.refill_per_second
                                                    for bucket in self.policy_buckets if bucket.is_request_bucket())
            self.expected_cost_per_request = 1
        else:
            elapsed_time = new_status_update_time - self.last_status_update_time

            requests_per_second_list = [bucket.count_cost_per_second(elapsed_time, new_content) for bucket, new_content in
                                        zip(self.policy_buckets, new_content_list) if bucket.is_request_bucket()]
            requests_per_second = max(requests_per_second_list)

            cost_per_second_list = [bucket.count_cost_per_second(elapsed_time, new_content) for bucket, new_content in
                                    zip(self.policy_buckets, new_content_list) if not bucket.is_request_bucket()]
            cost_per_request = max(cost_per_second_list) / min(requests_per_second_list)

            self._update_expected_requests_per_second(elapsed_time, requests_per_second, is_status=True)
            self._update_expected_cost_per_request(elapsed_time, cost_per_request, is_status=True)

        self.last_status_update_time = new_status_update_time

        for bucket, new_content in zip(self.policy_buckets, new_content_list):
            bucket.content = new_content

    def update(self, headers):
        """ Update expectations with headers
        """
        elapsed_time = time.time() - self.last_status_update_time
        is_rate_limited = self.VIOLATION_HEADER in headers
        # print(is_rate_limited, '???')
        if not is_rate_limited:
            self.requests_in_process -= 1

        if self.REQUEST_COUNT_HEADER not in headers:
            return

        remaining_requests = float(headers[self.REQUEST_COUNT_HEADER])
        requests_per_second = max(bucket.count_cost_per_second(elapsed_time, remaining_requests) for bucket in
                                  self.policy_buckets if bucket.is_request_bucket())

        self._update_expected_requests_per_second(elapsed_time, requests_per_second, is_rate_limited=is_rate_limited)

        if self.UNITS_COUNT_HEADER not in headers:
            return

        remaining_units = float(headers[self.UNITS_COUNT_HEADER])
        cost_per_second = max(bucket.count_cost_per_second(elapsed_time, remaining_units) for bucket in
                              self.policy_buckets if not bucket.is_request_bucket())
        cost_per_request = cost_per_second / requests_per_second

        self._update_expected_cost_per_request(elapsed_time, cost_per_request, is_rate_limited=is_rate_limited)

        # log_header(headers)

    def _update_expected_requests_per_second(self, elapsed_time, requests_per_second, is_status=False,
                                             is_rate_limited=False):
        """ This method updates the number of expected requests per second
        """
        value_difference = requests_per_second - self.expected_requests_per_second
        if is_rate_limited:
            # Making sure we cannot decrease expectations if we get rate-limited
            value_difference = max(value_difference, 0)

        severity_factor = 1
        if not (is_status or is_rate_limited):
            severity_factor = elapsed_time / self.last_status_update_time

        if value_difference < 0:
            # Decreasing expectations should be done slower
            severity_factor *= 0.5

        severity_factor = min(severity_factor, 1)
        self.expected_requests_per_second += value_difference * severity_factor

    def _update_expected_cost_per_request(self, elapsed_time, cost_per_request, is_status=False,
                                          is_rate_limited=False):
        """ This method updates the expected cost per request
        """  # TODO: resolve code duplications
        value_difference = cost_per_request - self.expected_cost_per_request
        if is_rate_limited:
            # Making sure we cannot decrease expectations if we get rate-limited
            value_difference = max(value_difference, 0)

        severity_factor = 1
        if not (is_status or is_rate_limited):
            severity_factor = elapsed_time / self.last_status_update_time

        if value_difference < 0:
            # Decreasing expectations should be done slower
            severity_factor *= 0.5

        severity_factor = min(severity_factor, 1)
        self.expected_cost_per_request += value_difference * severity_factor


class PolicyBucket:

    def __init__(self, policy_type, policy_payload):

        self.policy_type = PolicyType(policy_type)

        self.capacity = policy_payload['capacity']  # TODO: maybe cast to int
        self.refill_period = policy_payload['samplingPeriod']

        # The following is the same as if we would interpret samplingPeriod string
        self.refill_per_second = 10 ** 9 / policy_payload['nanosBetweenRefills']

        self.content = self.capacity

    def __repr__(self):
        return '{}(policy_type={}, capacity={}, refill_period={}, refill_per_second={})' \
               ''.format(self.__class__.__name__, self.policy_type, self.capacity, self.refill_period,
                         self.refill_per_second)

    def count_cost_per_second(self, elapsed_time, new_content):
        """ Counts how much has been taken from the bucket in the elapsed time
        """
        content_difference = self.content - new_content
        if not self.is_fixed():
            content_difference += elapsed_time * self.refill_per_second

        return content_difference / elapsed_time

    def get_expected_wait_time(self, elapsed_time, expected_requests_per_second, expected_cost_per_request,
                               requests_in_process):
        """ Expected time a user would have to wait for this bucket
        """
        expected_cost_per_second = expected_requests_per_second * expected_cost_per_request
        expected_content = self.content + elapsed_time * (self.refill_per_second - expected_cost_per_second) \
                           - requests_in_process * expected_cost_per_request

        if self.is_fixed():
            if expected_content < expected_cost_per_request:
                return -1
            return 0
        # print(expected_content, max(expected_cost_per_request - expected_content, 0) / self.refill_per_second, '!!!')
        return max(expected_cost_per_request - expected_content, 0) / self.refill_per_second

    def is_request_bucket(self):
        """ Checks if bucket counts requests
        """
        return self.policy_type is PolicyType.REQUESTS

    def is_fixed(self):
        """ Checks if bucket has a fixed number of requests
        """
        return self.refill_period == 'PT0S'


class PolicyType(Enum):
    """ Enum defining different types of policies
    """
    PROCESSING_UNITS = 'PROCESSING_UNITS'
    REQUESTS = 'REQUESTS'
