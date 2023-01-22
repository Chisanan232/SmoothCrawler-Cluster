# SmoothCrawler-Cluster

[![Supported Versions](https://img.shields.io/pypi/pyversions/SmoothCrawler-Cluster.svg?logo=python&logoColor=FBE072)](
https://pypi.org/project/SmoothCrawler-Cluster)
[![PyPI version](https://img.shields.io/pypi/v/SmoothCrawler-Cluster?color=%23099cec&amp;label=PyPI&amp;logo=pypi&amp;logoColor=white)](
https://pypi.org/project/SmoothCrawler-Cluster/)
[![Release](https://img.shields.io/github/release/Chisanan232/SmoothCrawler-Cluster.svg?label=Release&amp;logo=github&color=orange)](
https://github.com/Chisanan232/SmoothCrawler-Cluster/releases)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache)](https://opensource.org/licenses/Apache-2.0)
[![smoothcrawler-cluster ci/cd](https://github.com/Chisanan232/SmoothCrawler-Cluster/actions/workflows/ci-cd.yml/badge.svg)](
https://github.com/Chisanan232/SmoothCrawler-Cluster/actions/workflows/ci-cd.yml)
[![codecov](https://codecov.io/gh/Chisanan232/SmoothCrawler-Cluster/branch/master/graph/badge.svg?token=H34TPZQXYL)](
https://codecov.io/gh/Chisanan232/SmoothCrawler-Cluster)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-black)](https://github.com/PyCQA/pylint)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/171272bee2594687964f1f4473628a0f)](
https://www.codacy.com/gh/Chisanan232/SmoothCrawler-Cluster/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Chisanan232/SmoothCrawler-Cluster&amp;utm_campaign=Badge_Grade)

*SmoothCrawler-Cluster* is a Python framework which is encapsulation of building cluster or decentralized crawler system 
humanly with [*SmoothCrawler*](https://github.com/Chisanan232/smoothcrawler).

[Overview](#overview) | [Quickly Demo](#quickly-demo)
<hr>


## Overview

*SmoothCrawler* helps you build crawler with multiple components as combining LEGO. *SmoothCrawler-Cluster* helps you build
a cluster or decentralized system with the LEGO. It's same as the reason why *SmoothCrawler* exist: SoC (Separation of Concerns).
Developers could focus on how to handle everything of HTTP request and HTTP response, how to parse the content of HTTP response, etc.
In addiction to the crawler features, it also has the cluster or decentralized system feature.

## Quickly Demo

For the demonstration, it divides to 2 parts: 

* [_General crawler feature_](#general-crawler-feature)

    Demonstrate a general crawling feature, but doesn't have any features are relative with cluster or decentralized system.

* [_Cluster feature_](#cluster-feature)

    Here would let developers be aware of how it runs as a cluster system which is high reliability.

### _General crawler feature_

Currently, it only supports cluster feature with third party application [**_Zookeeper_**](https://zookeeper.apache.org/documentation.html).
So let's start to demonstrate with object **ZookeeperCrawler**:

```python
from smoothcrawler_cluster.crawler import ZookeeperCrawler

zk_crawler = ZookeeperCrawler(runner=1,    # How many crawler to run task
                              backup=1,    # How many crawler is backup of runner
                              ensure_initial=True,    # Run the initial process first
                              zk_hosts="localhost:2181")    # Zookeeper hosts
zk_crawler.register_factory(http_req_sender=RequestsHTTPRequest(),
                            http_resp_parser=RequestsExampleHTTPResponseParser(),
                            data_process=ExampleDataHandler())
zk_crawler.run()
```

It would run as an unlimited loop after calling *run*. If it wants to trigger the crawler instance to run crawling task, 
please assigning task via setting value to Zookeeper node.

> **Note**
> Please run the above Python code as 2 different processes, e.g., open 2 terminate tabs or windows and run above Python 
> code in each one.

```python
from kazoo.client import KazooClient
from smoothcrawler_cluster.model import Initial
import json

# Initial task data
task = Initial.task(running_content=[{
    "task_id": 0,
    "url": "https://www.example.com",
    "method": "GET",
    "parameters": {},
    "header": {},
    "body": {}
}])

# Set the task value
zk_client = KazooClient(hosts="localhost:2181")
zk_client.start()
zk_client.set(path="/smoothcrawler/node/sc-crawler_1/task", value=bytes(json.dumps(task.to_readable_object()), "utf-8"))
```

After assigning task to crawler instance, it would run the task and save the result back to Zookeeper.

```shell
[zk: localhost:2181(CONNECTED) 19] get /smoothcrawler/node/sc-crawler_1/task
{"running_content": [], "cookie": {}, "authorization": {}, "in_progressing_id": "-1", "running_result": {"success_count": 1, 
"fail_count": 0}, "running_status": "done", "result_detail": [{"task_id": 0, "state": "done", "status_code": 200, "response": 
"Example Domain", "error_msg": null}]}
```

From above info, we could get the running result detail in column *result_detail*: 

```json
[
  {
    "task_id": 0, 
    "state": "done", 
    "status_code": 200, 
    "response": "Example Domain", 
    "error_msg": null
  }
]
```

Above data means the task which *task_id* is 0 it has done, and the HTTP status code it got is 200. Also it got the parsing 
result: Example Domain.

### _Cluster feature_

Now we understand how to use it as web spider, but what does it mean below?

> ... how it runs as a cluster system which is high reliability.

Do you remember we run 2 crawler instances, right? Let's check the info about **GroupState** of these crawler instances: 

```shell
[zk: localhost:2181(CONNECTED) 10] get /smoothcrawler/group/sc-crawler-cluster/state
{"total_crawler": 2, "total_runner": 1, "total_backup": 1, "standby_id": "2", "current_crawler": ["sc-crawler_1", "sc-crawler_2"], 
"current_runner": ["sc-crawler_1"], "current_backup": ["sc-crawler_2"], "fail_crawler": [], "fail_runner": [], "fail_backup": []}
```

It shows that it only one instance is **Runner** and would receive tasks to run right now. So let's try to stop or kill the 
Runner one and observe the crawler instances behavior.

> **Note**
> If you opened 2 terminate tabs or windows to run, please select the first one you run and run control + C.

You would observe that the **Backup** one would activate by itself to be **Runner** and the original **Runner** one would 
be recorded at column *fail_crawler* and *fail_runner*.

```shell
[zk: localhost:2181(CONNECTED) 11] get /smoothcrawler/group/sc-crawler-cluster/state
{"total_crawler": 2, "total_runner": 1, "total_backup": 0, "standby_id": "3", "current_crawler": ["sc-crawler_2"], "current_runner": 
["sc-crawler_2"], "current_backup": [], "fail_crawler": ["sc-crawler_1"], "fail_runner": ["sc-crawler_1"], "fail_backup": []}
```

The crawler instance *sc-crawler_2* would be the new **Runner** one to wait for task and run. And you also could test its 
crawling feature as [_General crawler feature_](#general-crawler-feature).

So far, it demonstrates it besides helps developers to build web crawler as a clean software architecture, it also has cluster 
feature to let it be a high reliability crawler.
