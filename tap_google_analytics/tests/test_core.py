""" Core tests of tap-google-analytics """
import unittest
from unittest.mock import patch
from googleapiclient.http import RequestMockBuilder

import datetime
import os

import tap_google_analytics

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
def datafile(filename):
    return os.path.join(DATA_DIR, filename)
def readfile(filename):
    data = None
    if filename:
        f = open(filename, 'rb')
        data = f.read() 
        f.close()
    return data.decode("utf-8") 

class TestCore(unittest.TestCase):
    """ Test class for sync package """

    def setUp(self):
        """ Setup the test objects and helpers """
        self.requestBuilder = RequestMockBuilder({}) # all responses empty 200 OK

    def _mock_initialize_requestbuilder(self):
        return self.requestBuilder

    @patch('tap_google_analytics.ga_client.GAClient.initialize_requestbuilder')
    def test_bearer_discover(self, stub_initialize_requestbuilder):
        """ Test basic sync with Bearer Token"""
        # given all required config
        mock_config = {
            'view_id': '123456789',
            'start_date': datetime.datetime.now().strftime("%Y-%m-%d"),
            'end_date': datetime.datetime.now().strftime("%Y-%m-%d"),
            'authorization': { 'bearer_token': 'mock-token' },
        }
        # given mock successful response
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
