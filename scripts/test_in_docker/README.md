## Run testing through Docker

In development, we could use the scripts or config here to help us to run testing with each different Python version. *SmoothCrawler-Cluster* 
supports Python version 3.6 to 3.11 as below badges:

[![Supported Versions](https://img.shields.io/pypi/pyversions/SmoothCrawler-Cluster.svg?logo=python&logoColor=FBE072)](
https://pypi.org/project/SmoothCrawler-Cluster)

So it may have to run testing in different Python version runtime environment. In generally, it has 2 ways to do it: 

* **PyEnv**
* **Docker**

Both of 2 are good ways to do it. **PyEnv** is more convenience for using in local site and **Docker** is safer and more independent to run 
testing in a independent runtime environment. This directory provides developers some scripts they would need it when they use **Docker** way.

About shell script *run-pytest.sh*, it is for running in docker container to get all test items by testing type (unit-test or integration-test) 
and run it. And *pytest_Dockerfile* is for building docker image.

No matter whether you're new in Docker or not, it's very easy to use.

Please make sure that you have [installed Docker](https://docs.docker.com/engine/install/):

```shell
>>> docker info
```

If it does, let's build the docker image by the specific *Dockerfile*:

```shell
>>> docker build ./ -t pytest/sc-cluster:v1 -f ./scripts/test_in_docker/pytest_Dockerfile
[+] Building 37.4s (18/18) FINISHED
 => [internal] load build definition from pytest_Dockerfile                                          0.0s
 => => transferring dockerfile: 1.73kB                                                               0.0s
 => [internal] load .dockerignore                                                                    0.0s
 => => transferring context: 2B                                                                      0.0s
 => [internal] load metadata for docker.io/library/python:3.10                                       2.4s
 => [internal] load build context                                                                    0.5s
 => => transferring context: 21.66kB                                                                 0.5s
 => [ 1/13] FROM docker.io/library/python:3.10@sha256:6a9c89e338acace4a822ebbdaaf80ea86814a60115a90  3.2s
 => => resolve docker.io/library/python:3.10@sha256:6a9c89e338acace4a822ebbdaaf80ea86814a60115a9031  0.0s
 => => sha256:df16ad271c4d3233d37e2fcace80cebe294a0fc6b6993442725fc61052109fd2 20.09MB / 20.09MB     1.8s
 => => sha256:f056fbc065b764f83721b5b637723474109a730fc8b47ae068de1cc6dff878f2 3.06MB / 3.06MB       3.0s
 => => sha256:6a9c89e338acace4a822ebbdaaf80ea86814a60115a903120806576ac69db3a6 2.36kB / 2.36kB       0.0s
 => => sha256:8d1607679f3f93c3867c90dc0efb4b14354bfddca147331c62c5cf963ec5d68a 234B / 234B           0.7s
 => => sha256:5cce480ab66fc693f5c164a7b8f6648b57097b37f68afba8af3505d87c1745d3 2.22kB / 2.22kB       0.0s
 => => sha256:a443975c5dddf54767434efee1a32c73350e9096bb871b1f995c22b9adc24af1 8.90kB / 8.90kB       0.0s
 => => extracting sha256:df16ad271c4d3233d37e2fcace80cebe294a0fc6b6993442725fc61052109fd2            0.5s
 => => extracting sha256:8d1607679f3f93c3867c90dc0efb4b14354bfddca147331c62c5cf963ec5d68a            0.0s
 => => extracting sha256:f056fbc065b764f83721b5b637723474109a730fc8b47ae068de1cc6dff878f2            0.2s
 => [ 2/13] WORKDIR ./apache-smoothcrawler-cluster/                                                  0.1s
 => [ 3/13] RUN pip install -U pip                                                                   1.5s
 => [ 4/13] COPY ../../requirements/requirements.txt ./requirements/                                 0.0s
 => [ 5/13] RUN pip install -r ./requirements/requirements.txt                                       4.5s
 => [ 6/13] COPY ../../requirements/requirements-test.txt ./requirements/                            0.0s
 => [ 7/13] RUN pip install -U -r ./requirements/requirements-test.txt                              21.1s
 => [ 8/13] RUN apt-get update &&       apt-get install -y --no-install-recommends jq                4.0s
 => [ 9/13] COPY ../../smoothcrawler_cluster ./smoothcrawler_cluster/                                0.0s
 => [10/13] COPY ../../test ./test/                                                                  0.0s
 => [11/13] COPY ../../scripts ./scripts/                                                            0.0s
 => [12/13] COPY ../../.coveragerc ./                                                                0.1s
 => [13/13] COPY ../../pytest.ini ./                                                                 0.0s
 => exporting to image                                                                               0.3s
 => => exporting layers                                                                              0.3s
 => => writing image sha256:2c8f57efcfbadf51b814ca3808e395e9d7ecf90d114a1cda438b9e94f42ae3f7         0.0s
 => => naming to docker.io/pytest/sc-cluster:v1                                                      0.0s

Use 'docker scan' to run Snyk tests against images to find vulnerabilities and learn how to fix them
```

And verify the image has been already:

```shell
>>> docker images
REPOSITORY            TAG      IMAGE ID       CREATED          SIZE
pytest/sc-cluster     v1       2c8f57efcfba   23 seconds ago   994MB
```

After ready the docker image, we could start to run testing through Docker with the image:

```shell
>>> docker run --name sc-cluster_pytest -e TESTING_TYPE=unit-test -d pytest/sc-cluster:v1
```

Please don't forget pass the argument ``TESTING_TYPE`` into docker container, it would run the testing by the argument. Finally, you could 
verify the testing result by checking docker container log:

```shell
>>> docker logs -f sc-cluster_pytest
```
