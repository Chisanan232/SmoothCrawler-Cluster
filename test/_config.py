"""
Here are some testing settings
"""

import os

# # Zookeeper setting
Zookeeper_Hosts = os.getenv("PYTEST_ZOOKEEPER_HOSTS", default="localhost:2181")

# # Testing HTTP server settings
Test_Task_HTTP_Host = os.getenv("Test_Task_HTTP_Host", default="127.0.0.1:12345")
Test_Task_API_Path = os.getenv("Test_Task_API_Path", default="/example.com")
