""" GA client of tap-google-analytics """
import unittest
from unittest.mock import patch

import datetime
import http
import json

import tap_google_analytics
from tap_google_analytics.tests.utils import datafile
from tap_google_analytics.tests.utils import readfile
from tap_google_analytics.tests.utils import MockHttp
from tap_google_analytics.tests.utils import MockResponse


class TestGAClient(unittest.TestCase):
    """ Test class for ga_client """

    def setUp(self):
        """ Setup the test objects and helpers """
        self.mock_http = MockHttp(MockResponse()) # one empty 200 OK

    def _mock_initialize_http(self):
        return self.mock_http

    @patch('tap_google_analytics.ga_client.GAClient.initialize_http')
    def test_proxy_refresh_handler_no_refresh(self, stub_initialize_http):
        """ Test discover with proxy request that makes no refresh request"""
        # given all required config
        mock_config = {
            'view_id': '123456789',
            'start_date': datetime.datetime.now().strftime("%Y-%m-%d"),
            'end_date': datetime.datetime.now().strftime("%Y-%m-%d"),
            'oauth_credentials': { 
                'refresh_proxy_url': 'mock-url',
                'access_token': 'mock-token',
                'refresh_token': 'mock-refresh-token' 
            },
        }
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


    @patch('tap_google_analytics.ga_client.GAClient.initialize_http')
    def test_proxy_refresh_handler_refresh(self, stub_initialize_http):
        """ Test refresh via proxy request"""
        # given all required config
        mock_config = {
            'view_id': '123456789',
            'start_date': datetime.datetime.now().strftime("%Y-%m-%d"),
            'end_date': datetime.datetime.now().strftime("%Y-%m-%d"),
            'oauth_credentials': { 
                'refresh_proxy_url': 'https://localhost/tokens/oauth2-google/token',
                'refresh_proxy_url_auth': 'Bearer mock-bearer',
                'access_token': 'mock-token',
                'refresh_token': 'mock-refresh-token' 
            },
        }
        # given mock service documents
        mock_analyticsreporting_response = readfile(datafile('analyticsreporting-servicedocument.json'))
        mock_analytics_response = readfile(datafile('analytics-servicedocument.json'))
        # given mock token response
        mock_token_response = {
            "access_token": "1/fFAGRNJru1FTz70BzhT3Zg",
            "expires_in": 3920,
            "scope": "https://www.googleapis.com/auth/drive.metadata.readonly",
            "token_type": "Bearer"
        }
        # given mock successful metadata response        
        mock_metadata_response = readfile(datafile('analytics-metadata-columns-list.json'))
        # given mock http responses
        mock_http_metadata = MockHttp(
            [
                # GET https://www.googleapis.com/discovery/v1/apis/analyticsreporting/v4/rest
                MockResponse(data=mock_analyticsreporting_response),
                # GET https://www.googleapis.com/discovery/v1/apis/analytics/v3/rest
                MockResponse(data=mock_analytics_response),
                # 401 token needs refreshing
                MockResponse(status=http.HTTPStatus.UNAUTHORIZED),
                # 200 token response from POST 
                MockResponse(status=http.HTTPStatus.OK, data=json.dumps(mock_token_response)),
                # GET https://analytics.googleapis.com/analytics/v3/metadata/ga/columns?alt=json
                MockResponse(data=mock_metadata_response)
            ]
        )
        self.mock_http = mock_http_metadata
        stub_initialize_http.side_effect = self._mock_initialize_http

        # when discover default catalog with 'oauth_credentials.refresh_proxy_url'
        catalog = tap_google_analytics.discover(mock_config)
        
        # expect refresh_proxy_url_auth to be used in token request
        token_request = self.mock_http.requests[3]
        token_request_headers = token_request[3]
        self.assertEqual(token_request_headers['authorization'], "Bearer mock-bearer")

        # expect access_token response to be used in metadata request
        metadata_request = self.mock_http.requests[4]
        metadata_request_headers = metadata_request[3]
        self.assertEqual(metadata_request_headers['authorization'], "Bearer 1/fFAGRNJru1FTz70BzhT3Zg")

        # expect valid catalog to be discovered
        self.assertEqual(len(catalog['streams']), 10, "Total streams from default catalog")
