""" Core tests of tap-google-analytics """
import unittest
from unittest.mock import patch
from googleapiclient.http import RequestMockBuilder

import datetime

import tap_google_analytics
from tap_google_analytics.tests.utils import datafile
from tap_google_analytics.tests.utils import readfile
from tap_google_analytics.tests.utils import MockHttp
from tap_google_analytics.tests.utils import MockResponse

class TestCore(unittest.TestCase):
    """ Test class for core tests """

    def setUp(self):
        """ Setup the test objects and helpers """
        self.requestBuilder = RequestMockBuilder({}) # all responses empty 200 OK

    def _mock_initialize_requestbuilder(self):
        return self.requestBuilder

    def _mock_initialize_http(self):
        mock_analyticsreporting_response = readfile(datafile('analyticsreporting-servicedocument.json'))
        mock_analytics_response = readfile(datafile('analytics-servicedocument.json'))
        self.mock_http = MockHttp(
            [
                # GET https://www.googleapis.com/discovery/v1/apis/analyticsreporting/v4/rest
                MockResponse(data=mock_analyticsreporting_response),
                # GET https://www.googleapis.com/discovery/v1/apis/analytics/v3/rest
                MockResponse(data=mock_analytics_response)
            ]
        )
        return self.mock_http

    @patch('tap_google_analytics.ga_client.GAClient.initialize_http')
    @patch('tap_google_analytics.ga_client.GAClient.initialize_requestbuilder')
    def test_bearer_discover(self, stub_initialize_requestbuilder, stub_initialize_http):
        """ Test basic discover sync with Bearer Token"""
        # given mock service documents
        stub_initialize_http.side_effect = self._mock_initialize_http
        # given all required config
        mock_config = {
            'view_id': '123456789',
            'start_date': datetime.datetime.now().strftime("%Y-%m-%d"),
            'end_date': datetime.datetime.now().strftime("%Y-%m-%d"),
            'authorization': { 'bearer_token': 'mock-token' },
        }
        # given mock metadata successful response
        stub_initialize_requestbuilder.side_effect = self._mock_initialize_requestbuilder
        mock_metadata_response = readfile(datafile('analytics-metadata-columns-list.json'))
        self.requestBuilder = RequestMockBuilder(
            {"analytics.metadata.columns.list": 
                (None, mock_metadata_response)
            }
        )

        # when discover default catalog with 'authorization.bearer_token'
        catalog = tap_google_analytics.discover(mock_config)

        # expect valid catalog to be discovered
        self.assertEqual(len(catalog['streams']), 10, "Total streams from default catalog")

