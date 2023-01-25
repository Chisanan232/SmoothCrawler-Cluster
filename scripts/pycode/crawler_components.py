from typing import Any

import requests
from bs4 import BeautifulSoup
from smoothcrawler.components.data import BaseDataHandler, BaseHTTPResponseParser
from smoothcrawler.components.httpio import HTTP


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
