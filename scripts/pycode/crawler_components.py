from bs4 import BeautifulSoup
from smoothcrawler.components.httpio import HTTP
from smoothcrawler.components.data import BaseHTTPResponseParser, BaseDataHandler
from typing import Any
import requests
import os


# Test_Example_URL = "http://www.example.com/"
_CRAWLER_MODE = os.getenv("CRAWLER_MODE", "general")
TEST_EXAMPLE_URL = os.getenv("TEST_URL", "http://www.example.com/")
_RUNNER_CRAWLER_VALUE = int(os.getenv("CLUSTER_RUNNER", 2))
_BACKUP_CRAWLER_VALUE = int(os.getenv("CLUSTER_BACKUP", 1))
_CRAWLER_NAME = os.getenv("CRAWLER_NAME", "sc-crawler_1")
_ZK_HOSTS = os.getenv("ZK_HOSTS", "localhost:2181")


class RequestsHTTPRequest(HTTP):

    _http_response = None

    def get(self, url: str, *args, **kwargs):
        self._http_response = requests.get(url)
        return self._http_response


class RequestsExampleHTTPResponseParser(BaseHTTPResponseParser):

    def get_status_code(self, response: requests.Response) -> int:
        return response.status_code

    def handling_200_response(self, response: requests.Response) -> Any:
        bs = BeautifulSoup(response.text, "html.parser")
        example_web_title = bs.find_all("h1")
        return example_web_title[0].text


class ExampleDataHandler(BaseDataHandler):

    def process(self, result: Any) -> Any:
        return result

