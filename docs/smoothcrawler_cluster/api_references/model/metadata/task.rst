======
Task
======

.. _MetaData_Task:

In meta-data object **Task**, it has some option has complex structure value like option *running_content*,
*running_result* and *result_detail*. The value in these options is dict type value, even is a list of dict
type element. For operations with these complex options, it may have some unexpected behavior or hardcode.
So here are 3 objects for resolving these issues and let usage to be more expected and easily to maintain.

.. autodata:: smoothcrawler_cluster.model.metadata.RunningContent
.. autodata:: smoothcrawler_cluster.model.metadata.RunningResult
.. autodata:: smoothcrawler_cluster.model.metadata.ResultDetail

.. autoclass:: smoothcrawler_cluster.model.metadata.Task
    :members:
