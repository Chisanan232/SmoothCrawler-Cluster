<h1 align="center">
  SmoothCrawler-Cluster
</h1>

<p align="center">
  <a href="https://pypi.org/project/SmoothCrawler-Cluster">
    <img src="https://img.shields.io/pypi/pyversions/SmoothCrawler-Cluster.svg?logo=python&logoColor=FBE072" alt="PyPI support versions">
  </a>
  <a href="https://pypi.org/project/SmoothCrawler-Cluster">
    <img src="https://img.shields.io/pypi/v/SmoothCrawler-Cluster?color=%23099cec&amp;label=PyPI&amp;logo=pypi&amp;logoColor=white" alt="PyPI package version">
  </a>
  <a href="https://github.com/Chisanan232/SmoothCrawler-Cluster/releases">
    <img src="https://img.shields.io/github/release/Chisanan232/SmoothCrawler-Cluster.svg?label=Release&amp;logo=github&color=orange" alt="GitHub release version">
  </a>
  <a href="https://opensource.org/licenses/Apache-2.0">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache" alt="Software license">
  </a>
  <a href="https://github.com/Chisanan232/SmoothCrawler-Cluster/actions/workflows/ci-cd.yml">
    <img src="https://github.com/Chisanan232/SmoothCrawler-Cluster/actions/workflows/ci-cd.yml/badge.svg" alt="CI/CD status">
  </a>
  <a href="https://codecov.io/gh/Chisanan232/SmoothCrawler-Cluster">
    <img src="https://codecov.io/gh/Chisanan232/SmoothCrawler-Cluster/branch/master/graph/badge.svg?token=H34TPZQXYL" alt="Test coverage">
  </a>
  <a href="https://github.com/psf/black">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Coding style reformat tool">
  </a>
  <a href="https://github.com/PyCQA/pylint">
    <img src="https://img.shields.io/badge/linting-pylint-yellowgreen" alt="Coding style checking tool">
  </a>
  <a href="https://results.pre-commit.ci/latest/github/Chisanan232/SmoothCrawler-Cluster/master">
    <img src="https://results.pre-commit.ci/badge/github/Chisanan232/SmoothCrawler-Cluster/master.svg" alt="Pre-Commit building state">
  </a>
  <a href="https://www.codacy.com/gh/Chisanan232/SmoothCrawler-Cluster/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Chisanan232/SmoothCrawler-Cluster&amp;utm_campaign=Badge_Grade">
    <img src="https://app.codacy.com/project/badge/Grade/171272bee2594687964f1f4473628a0f" alt="Code quality level">
  </a>
  <a href='https://smoothcrawler-cluster.readthedocs.io/en/latest/?badge=latest'>
      <img src='https://readthedocs.org/projects/smoothcrawler-cluster/badge/?version=latest' alt='Documentation Status' />
  </a>

</p>

<p align="center">
  <em>SmoothCrawler-Cluster</em> is a Python framework which is encapsulation of building cluster or decentralized crawler system
  humanly with <a href="https://github.com/Chisanan232/smoothcrawler"><em>SmoothCrawler</em></a>.
</p>

[Overview](#overview) | [Quickly Demo](#quickly-demo) | [Documentation](#documentation)
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


## Documentation

The [documentation](https://smoothcrawler-cluster.readthedocs.io) contains more details, and demonstrations.

* [Quickly Start](https://smoothcrawler-cluster.readthedocs.io/en/master/quickly_start.html) to build your own crawler cluster with *SmoothCrawler-Cluster*
* Detail *SmoothCrawler-Cluster* usage information of functions, classes and methods in [API References](https://smoothcrawler-cluster.readthedocs.io/en/master/index.html#api-reference)
* I'm clear what I need and want to [customize something](https://smoothcrawler-cluster.readthedocs.io/en/master/advanced_usage/index.html) of *SmoothCrawler-Cluster*
* Not sure how to use *SmoothCrawler-Cluster* and design your crawler cluster? [Usage Guides](https://smoothcrawler-cluster.readthedocs.io/en/master/index.html#usage-guides) could be a good guide for you
* Be curious about the details of *SmoothCrawler-Cluster* development? [Development Documentation](https://smoothcrawler-cluster.readthedocs.io/en/master/index.html#development-documentation) would be helpful to you
* The [Release Notes](https://smoothcrawler-cluster.readthedocs.io/en/master/index.html#change-logs) of *SmoothCrawler-Cluster*


## Download

*SmoothCrawler* still a young open source which keep growing. Here's its download state:

[![Downloads](https://pepy.tech/badge/smoothcrawler-cluster)](https://pepy.tech/project/smoothcrawler-cluster)
[![Downloads](https://pepy.tech/badge/smoothcrawler-cluster/month)](https://pepy.tech/project/smoothcrawler-cluster)
