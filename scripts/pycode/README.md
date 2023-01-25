## Example of *SmoothCrawler-CLuster*

In this directory, it has some demonstrations for how to use *SmoothCrawler-Cluster* and an example Dockerfile and docker-compose to 
help you set up a crawler cluster to more understand how it works.

It could divide the examples to 3 parts: 

* Task assigner
* Task runner (it includes *SmoothCrawler* components and crawler main body)
* Docker configurations (it includes docker-compose)

### Task assigner

It is for assigning task for task runner. Currently, *SmoothCrawler-CLuster* only supports one crawler --- **ZookeeperCrawler**, so it 
must communicate with each other by Zookeeper. And this is responsible for passing the task to Zookeeper to let task runner could 
receive the task to process.

So we could run the Python scripty *assign_task.py* and verify its running result in Zookeeper:

At terminal:

```shell
>>> python3 ./scripts/pycode/assign_task.py
```

In Zookeeper, demonstrate with one of node's property **Task**:

```shell
>>>[zkshell: 1] get /smoothcrawler/node/sc-crawler_1/task
{"running_content": [], "cookie": {}, "authorization": {}, "in_progressing_id": "-1", "running_result": {"success_count": 0, "fail_count": 0}, "running_status": "nothing", "result_detail": []}
```

> **Note**
> Please remember that it needs to access into command line interface of Zookeeper by run ``./bin/zkCli.sh``.

### Task runner

The main body of sample crawler, and this sample be as *crawler_main.py*. For let it supports run as standalone, so it has 5 environment 
variables could use:

* CLUSTER_RUNNER: The instance amount for running task. Default is ``2``.
* CLUSTER_BACKUP: The instance amount for backup to standby for running task. Default is ``1``.
* CRAWLER_NAME: The name of crawler instance. Default is ``"sc-crawler_1""``.
* ZK_HOSTS: Zookeeper hosts (IP address with port, use comma to separate multiple values). Default is ``localhost:2181``.
* RUN_ONE_TASK: Boolean value. If it's *true*, it would only run one default sample task and won't run as one component of crawler cluster. Default is ``true``.

We could run the sample directly because it's default is running one task directly, doesn't receive task from Zookeeper.

```shell
>>> python3 ./scripts/pycode/crawler_main.py
Example Domain
```

### Docker configurations

This part is the most important of this example because it would demonstrate the value of package *SmoothCrawler-CLuster*. It needs to 
set up and run multiple crawler instances if we want to know its value. However, running multiple instances in the same time is a little 
bother developers. So this example provides configurations of docker and docker-compose to run multiple crawler instances and set up 
crawler cluster so that developers could be lazy as possible.

File *sc-crawler_Dockerfile* for building the image of running crawler, and it has the above arguments could use. And for the YAML format 
file *sc-cluster_docker-compose.yaml*, it could run multiple crawler instances and set up crawler cluster.

Let's demonstrate how to quickly start with these examples!

First, we need to build the docker image of sample crawler:

```shell
>>> docker build ./ -t client/sc-cluster:v1 -f ./scripts/pycode/sc-crawler_Dockerfile
[+] Building 38.2s (18/18) FINISHED
 => [internal] load build definition from sc-crawler_Dockerfile                                      0.0s
 => => transferring dockerfile: 1.15kB                                                               0.0s
 => [internal] load .dockerignore                                                                    0.0s
 => => transferring context: 2B                                                                      0.0s
 => [internal] load metadata for docker.io/library/python:3.10                                       4.0s
 => [ 1/13] FROM docker.io/library/python:3.10@sha256:5ef345608493927ad12515e75ebe0004f5633dd5d7b08  0.0s
 => => resolve docker.io/library/python:3.10@sha256:5ef345608493927ad12515e75ebe0004f5633dd5d7b08c1  0.0s
 => [internal] load build context                                                                    8.9s
 => => transferring context: 245.88MB                                                                8.9s
 => CACHED [ 2/13] WORKDIR ./apache-smoothcrawler-cluster/                                           0.0s
 => CACHED [ 3/13] RUN pip install -U pip                                                            0.0s
 => CACHED [ 4/13] COPY ./requirements/requirements.txt ./requirements/                              0.0s
 => CACHED [ 5/13] RUN pip install -r ./requirements/requirements.txt                                0.0s
 => CACHED [ 6/13] COPY ./requirements/requirements-test.txt ./requirements/                         0.0s
 => [ 7/13] RUN pip install -r ./requirements/requirements-test.txt                                 13.5s
 => [ 8/13] RUN apt-get update &&       apt-get install -y --no-install-recommends jq                3.9s
 => [ 9/13] RUN apt-get install -y iputils-ping &&       apt-get install -y net-tools &&       apt-  3.8s
 => [10/13] COPY ./ ./                                                                               1.5s
 => [11/13] COPY ./scripts ./scripts/                                                                0.1s
 => [12/13] COPY ./setup.py ./setup.py                                                               0.0s
 => [13/13] RUN python3 setup.py install                                                             0.9s
 => exporting to image                                                                               1.5s
 => => exporting layers                                                                              1.4s
 => => writing image sha256:c5a49eb71b71e801ed79bb7e48dec215d65ce037f133cc506fa79e074f270f75         0.0s
 => => naming to docker.io/client/sc-cluster:v1                                                      0.0s

Use 'docker scan' to run Snyk tests against images to find vulnerabilities and learn how to fix them
```

We could be careful to verify whether the image is ready or not:

```shell
>>> docker images
REPOSITORY            TAG       IMAGE ID       CREATED          SIZE
client/sc-cluster     v1        c5a49eb71b71   21 seconds ago   1.24GB
```

After assuring yourself that the docker image has been ready, it could run the docker-compose to set up a sample cluster with YAML format file:

```shell
>>> docker-compose -f ./scripts/pycode/sc-cluster_docker-compose.yaml up
[+] Running 6/2
 ⠿ Network pycode_sc-cluster_client_network  Created                                                 0.0s
 ⠿ Container test-zookeeper                  Created                                                 0.0s
 ⠿ Container sc-cluster_client_2             Created                                                 0.1s
 ⠿ Container sc-cluster_client_4             Created                                                 0.1s
 ⠿ Container sc-cluster_client_1             Created                                                 0.1s
 ⠿ Container sc-cluster_client_3             Created                                                 0.1s
Attaching to sc-cluster_client_1, sc-cluster_client_2, sc-cluster_client_3, sc-cluster_client_4, test-zookeeper
test-zookeeper       | ZooKeeper JMX enabled by default
test-zookeeper       | Using config: /conf/zoo.cfg
test-zookeeper       | 2023-01-24 09:26:13,517 [myid:] - INFO  [main:o.a.z.s.q.QuorumPeerConfig@177] - Reading configuration from: /conf/zoo.cfg
test-zookeeper       | 2023-01-24 09:26:13,519 [myid:] - INFO  [main:o.a.z.s.q.QuorumPeerConfig@431] - clientPort is not set
test-zookeeper       | 2023-01-24 09:26:13,520 [myid:] - INFO  [main:o.a.z.s.q.QuorumPeerConfig@444] - secureClientPort is not set
test-zookeeper       | 2023-01-24 09:26:13,520 [myid:] - INFO  [main:o.a.z.s.q.QuorumPeerConfig@460] - observerMasterPort is not set
```

It would have so many log messages be explored. We could verify the container status by command ``docker container``:

```shell
>>> docker container ps -a
CONTAINER ID   IMAGE                  COMMAND                  CREATED         STATUS                            PORTS        NAMES
102c3ce83e1d   client/sc-cluster:v3   "/bin/sh -c 'python3…"   6 seconds ago   Created                                        sc-cluster_client_4
8f3e9d1ac338   client/sc-cluster:v3   "/bin/sh -c 'python3…"   6 seconds ago   Created                                        sc-cluster_client_2
6d29bccda569   client/sc-cluster:v3   "/bin/sh -c 'python3…"   6 seconds ago   Created                                        sc-cluster_client_1
8d544f2f89be   client/sc-cluster:v3   "/bin/sh -c 'python3…"   6 seconds ago   Created                                        sc-cluster_client_3
d8fd7b40b84b   zookeeper:latest       "/docker-entrypoint.…"   6 seconds ago   Up 6 seconds (health: starting)                test-zookeeper
```

> **Hint**
> The command ``docker ps -a`` has the same function.

Finally, we could check the crawler cluster's running state by Zookeeper:

```shell
>>>[zkshell: 2] get /smoothcrawler/group/sc-crawler-cluster/state
{"total_crawler": 3, "total_runner": 2, "total_backup": 1, "standby_id": "3", "current_crawler": ["sc-crawler_1", "sc-crawler_2", "sc-crawler_3"], "current_runner": ["sc-crawler_1", "sc-crawler_2", "sc-crawler_1", "sc-crawler_2", "sc-crawler_1", "sc-crawler_2"], "current_backup": ["sc-crawler_3", "sc-crawler_3", "sc-crawler_3"], "fail_crawler": [], "fail_runner": [], "fail_backup": []}
```
