from smoothcrawler_cluster.__pkg_info__ import (
    __title__, __version__, __description__, __url__, __license__, __author__, __author_email__, __copyright__
)


_Expected_Title: str = "SmoothCrawler-Cluster"
_Expected_Description: str = "Develop and build web spider cluster humanly."
_Expected_URL: str = "https://smoothcrawler-cluster.readthedocs.io"
_Expected_Version: str = "0.1.0"
_Expected_Author: str = "Liu, Bryant"
_Expected_Author_Email: str = "chi10211201@cycu.org.tw"
_Expected_License: str = "Apache License 2.0"
_Expected_Copyright: str = "Copyright 2022 Bryant Liu"


def test_title() -> None:
    assert __title__ == _Expected_Title, "The title info should be same as expected value."


def test_description() -> None:
    assert __description__ == _Expected_Description, "The description info should be same as expected value."


def test_url() -> None:
    assert __url__ == _Expected_URL, "The URL should be same as expected value."


def test_version() -> None:
    assert __version__ == _Expected_Version, "The version info should be same as expected value."


def test_author() -> None:
    assert __author__ == _Expected_Author, "The author should be same as expected value."


def test_author_email() -> None:
    assert __author_email__ == _Expected_Author_Email, "The author email should be same as expected value."


def test_license() -> None:
    assert __license__ == _Expected_License, "The license should be same as expected value."


def test_copyright() -> None:
    assert __copyright__ == _Expected_Copyright, "The copyright info should be same as expected value."
