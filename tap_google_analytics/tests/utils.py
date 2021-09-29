""" Utilities used in this module """
import os
from unittest.mock import Mock

import httplib2
from http import HTTPStatus

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

class MockHttp(object):
    def __init__(self, responses, headers=None):
        self.responses = responses
        self.requests = []
        self.headers = headers or {}
        self.add_certificate = Mock(return_value=None)

    def request(
        self,
        url,
        method="GET",
        body=None,
        headers=None,
        redirections=httplib2.DEFAULT_MAX_REDIRECTS,
        connection_type=None,
    ):
        self.requests.append(
            (method, url, body, headers, redirections, connection_type)
        )
        return self.responses.pop(0)


class MockResponse(object):
    def __init__(self, status=HTTPStatus.OK, data=b""):
        self.status = status
        self.data = data

    def __iter__(self):
        yield self
        yield self.data
        
