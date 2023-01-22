### 🎉🎊🍾 New feature
<hr>

First Python library for crawler cluster be born into this world!

#### Source Code

1. Meta-data objects for every crawler instances communicates with each other in cluster.
2. Utility functions.
   2-1. Converting features about serialization and deserialization.
   3-2. Operations with Zookeeper.
3. Election function.
   3-1. Design base class and implement first and only one election --- **IndexElection**.
4. Crawler --- **ZookeeperCrawler**.

#### Test

1. Add configuration of *PyTest*.
2. Add configuration of calculating testing coverage of source code.
3. Unit test.
4. Integration test.

#### Documentation

1. Add docstring in source code includes *module*, *class*, *function* and *global variable*.
2. Add package documentation with *Sphinx*.

#### Configuration

1. Project management *Poetry*.
2. Coding style checking tool *PyLint*.
3. Service *CodeCov*.
4. Python pacakge *setup.py*.
5. Software license *APACHE 2.0*.
6. CI *GitHub Action* workflow and PR template.
7. Documentation CI *ReadTheDoc*.
8. Task automation tool *Tox*.
