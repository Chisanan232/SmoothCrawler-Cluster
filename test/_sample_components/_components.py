from bs4 import BeautifulSoup
from multirunnable.persistence.file import SavingStrategy
from typing import Any
from smoothcrawler.components.data import (
    # General component
    BaseHTTPResponseParser, BaseDataHandler,
    # Asynchronous version of general component
    BaseAsyncHTTPResponseParser, BaseAsyncDataHandler)
from smoothcrawler.components.httpio import HTTP, AsyncHTTP
from smoothcrawler.components.persistence import PersistenceFacade
import aiohttp
import requests
import urllib3

from ._persistence_layer import StockDao, StockFao


class Urllib3HTTPRequest(HTTP):

    _http_response = None

    def get(self, url: str, *args, **kwargs):
        http = urllib3.PoolManager()
        self._http_response = http.request("GET", url)
        return self._http_response


class RequestsHTTPRequest(HTTP):

    _http_response = None

    def get(self, url: str, *args, **kwargs):
        self._http_response = requests.get(url)
        return self._http_response


class AsyncHTTPRequest(AsyncHTTP):

    _http_response = None

    async def get(self, url: str, *args, **kwargs):
        async with aiohttp.ClientSession() as async_sess:
            resp = await async_sess.get(url)
            return resp


class Urllib3HTTPResponseParser(BaseHTTPResponseParser):

    def get_status_code(self, response: urllib3.response.HTTPResponse) -> int:
        return response.status

    def handling_200_response(self, response: urllib3.response.HTTPResponse) -> Any:
        bs = BeautifulSoup(response.read(), "html.parser")
        example_web_title = bs.find_all("h1")
        return example_web_title


class RequestsHTTPResponseParser(BaseHTTPResponseParser):

    def get_status_code(self, response: requests.Response) -> int:
        return response.status_code

    def handling_200_response(self, response: requests.Response) -> Any:
        bs = BeautifulSoup(response.text, "html.parser")
        example_web_title = bs.find_all("h1")[0].text
        return example_web_title


class AsyncHTTPResponseParser(BaseAsyncHTTPResponseParser):

    async def get_status_code(self, response: aiohttp.client.ClientResponse) -> int:
        return response.status

    async def handling_200_response(self, response: aiohttp.client.ClientResponse) -> Any:
        html = await response.text()
        bs = BeautifulSoup(html, "html.parser")
        example_web_title = bs.find_all("h1")
        response.release()
        return example_web_title

    async def handling_not_200_response(self, response: aiohttp.client.ClientResponse) -> Any:
        return response


class ExampleWebDataHandler(BaseDataHandler):

    def process(self, result):
        return result


class ExampleWebAsyncDataHandler(BaseAsyncDataHandler):

    async def process(self, result):
        return result


class DataFilePersistenceLayer(PersistenceFacade):

    def save(self, data, *args, **kwargs):
        stock_fao = StockFao(strategy=SavingStrategy.ONE_THREAD_ONE_FILE)
        stock_fao.save(formatter="csv", file="/Users/bryantliu/Downloads/stock_crawler_2330.csv", mode="a+", data=data)


class DataDatabasePersistenceLayer(PersistenceFacade):

    def save(self, data, *args, **kwargs):
        stock_dao = StockDao()
        stock_dao.create_stock_data_table(stock_symbol="2330")
        data_rows = [tuple(d) for d in data]
        stock_dao.batch_insert(stock_symbol="2330", data=data_rows)

