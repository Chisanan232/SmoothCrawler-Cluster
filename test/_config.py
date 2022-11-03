"""
Here are some testing settings
"""

import os

Zookeeper_Hosts = os.getenv("PYTEST_ZOOKEEPER_HOSTS", default="localhost:2181")
