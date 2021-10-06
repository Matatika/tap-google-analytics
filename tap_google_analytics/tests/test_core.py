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

        # when discover default catalog with 'authorization.bearer_token'
        catalog = tap_google_analytics.discover(mock_config)

        # expect valid catalog to be discovered
        self.assertEqual(len(catalog['streams']), 10, "Total streams from default catalog")


    @patch('tap_google_analytics.ga_client.GAClient.initialize_http')
    @patch('tap_google_analytics.ga_client.GAClient.initialize_requestbuilder')
    def test_client_id_and_client_secret_with_refresh_proxy_url_discover(self,stub_initialize_requestbuilder , stub_initialize_http):
        """ Test basic discover sync with client_id and client_secret"""
        # given all required config
        mock_config = {
            'view_id': '123456789',
            'start_date': datetime.datetime.now().strftime("%Y-%m-%d"),
            'oauth_credentials': {
                'refresh_proxy_url': 'mock-url',
                'access_token': 'mock-token',
                'refresh_token': 'mock-refresh-token',
                'client_id': '123456789',
                'client_secret' : '123456789'
            },
        }

        # given mock service documents
        mock_analyticsreporting_response = readfile(datafile('analyticsreporting-servicedocument.json'))
        mock_analytics_response = readfile(datafile('analytics-servicedocument.json'))
        # given mock metadata successful response
        stub_initialize_requestbuilder.side_effect = self._mock_initialize_requestbuilder
        mock_metadata_response = readfile(datafile('analytics-metadata-columns-list.json'))
        self.requestBuilder = RequestMockBuilder(
            {"analytics.metadata.columns.list": 
                (None, mock_metadata_response)
            }
        )
        # given mock service documents
        mock_analyticsreporting_response = readfile(datafile('analyticsreporting-servicedocument.json'))
        mock_analytics_response = readfile(datafile('analytics-servicedocument.json'))
        # given mock successful metadata response        
        mock_metadata_response = readfile(datafile('analytics-metadata-columns-list.json'))
        # given mock http responses
        mock_http_metadata = MockHttp(
            [
                # GET https://www.googleapis.com/discovery/v1/apis/analyticsreporting/v4/rest
                MockResponse(data=mock_analyticsreporting_response),
                # GET https://www.googleapis.com/discovery/v1/apis/analytics/v3/rest
                MockResponse(data=mock_analytics_response),
                # GET https://analytics.googleapis.com/analytics/v3/metadata/ga/columns?alt=json
                MockResponse(data=mock_metadata_response)
            ]
        )
        self.mock_http = mock_http_metadata
        stub_initialize_http.side_effect = self._mock_initialize_http

        # when discover default catalog with 'oauth_credentials.refresh_proxy_url'
        catalog = tap_google_analytics.discover(mock_config)

        # expect valid catalog to be discovered
        self.assertEqual(len(catalog['streams']), 10, "Total streams from default catalog")