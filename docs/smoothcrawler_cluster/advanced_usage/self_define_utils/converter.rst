===========================
How to write new Converter
===========================

In *SmoothCrawler-Cluster* package, it would process serialization features through *JSON* format data in default. Absolutely, it
also could customize your own serialization feature and apply it in **ZookeeperCrawler** via option *zk_converter*.

Before demonstrate, there are 3 things you need to know:

* Apart from serialize/deserialize feature, it must to implement the detail how to convert string type value into target meta-data objects.
* Although it needs to implement each one meta-data deserialization, it doesn't for serialization.
* For serialization, it has only one common function for processing all objects to target data format.

From above 3 points, that's clear let us to customized your own converter.

No matter which converters of meta-data, it must needs to extend the base class and implement all functions it rules:

.. code-block:: python

    from smoothcrawler_cluster._utils.converter import BaseConverter

    class ListConverter(BaseConverter):
        # all function implementations of converter

Let's start to learn how to implement new converter with ``list <-> str`` example!


Implement serialization/deserialization
=========================================

About serialization and deserialization features, the first we need to do must be how to convert an object to a string type value and
convert a string type value back to an object.


Serialization
---------------
Format objects as target data format
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before running serialization, it should let the data to be satisfied the format we want it to be. So we need to format it before serialize:

.. code-block:: python

    def _convert_to_readable_object(self, obj: Generic[_BaseMetaDataType]) -> Any:
        dict_obj: Dict[str, Any] = obj.to_readable_object()
        return list(dict_obj.values())

``obj.to_readable_object()`` would return a dict type value which format like as JSON. And it gets all values of the dict object and convert
to list directly. It has done the format part so that we could go ahead to implement serialization feature.

Serialize to string value
^^^^^^^^^^^^^^^^^^^^^^^^^^

About serialization, it means that convert an object to a string type which could save all needed data clearly. So for converting list
to str, we just need to use native function ``str()``:

.. code-block:: python

    def _convert_to_str(self, data: Any) -> str:
        data = str(data)
        return data

Now, we have done the whole feature of serialization. Let's keep implementing the deserialization part!

Deserialization
-----------------
Deserialize back to object
^^^^^^^^^^^^^^^^^^^^^^^^^^^

About deserialization, it should convert a string type value back to Python object, e.g., ``list`` in this demonstration. So we do it through
Python native library *ast*:

.. code-block:: python

    import ast

    def _convert_from_str(self, data: str) -> Any:
        parsed_data: List[Any] = ast.literal_eval(ini_list)
        return parsed_data

However, it doesn't finish the all tasks you should implement in deserialization. Above code only convert a string type value back to
a Python object --- ``list`` object, but it doesn't convert it to **meta-data object** yet. We still should implement the details of
how to convert it again to each one meta-data objects so that crawler could work finely with these meta-data objects with Zookeeper
through the customized converter you define.

Therefore, let's keep doing for converting of each one meta-data objects!

Detail of how to convert each one meta-data objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

About implementing the converting feature of each meta-data objects, it's the same mostly, the only different is the index or key to get
target value. So we only demonstrate **NodeState** part implementation here.

.. code-block:: python

    def _convert_to_node_state(self, state: NodeState, data: Any) -> NodeState:
        data: Dict[str, Any] = data
        state.group = data[0]
        state.role = data[1]
        return state

.. hint::

    It has different functions for implementing deserialization of different meta-data objects.

    * **GroupState** -> *_convert_to_group_state*
    * **NodeState** -> *_convert_to_node_state*
    * **Task** -> *_convert_to_task*
    * **Heartbeat** -> *_convert_to_heartbeat*

Congratulation! You finish a customized converter for ``list`` object. Let's try to use it and verify the running result!


Verify the converting features
================================

Finish all tasks we should do of implementing your own customized converter. Let's try to run it and verify whether the running result is
expected for us or not.

We could create a **NodeState** object for example through ``smoothcrawler_cluster.model.Initial``. It provides initialization function for
every meta-data objects. And we could test the customized converter features by functions ``serialize_meta_data`` and  ``deserialize_meta_data``
as below demonstration:

.. code-block:: python

    from smoothcrawler_cluster.model import NodeState, CrawlerStateRole, Initial

    node_state = Initial.node_state(group="test", role=CrawlerStateRole.RUNNER)

    # Instantiate your customized converter
    converter = ListConverter()

    # Test for serialization
    value = converter.serialize_meta_data(obj=node_state)
    print(f"value: {value}")

    # Test for deserialization
    metadata_obj = converter.deserialize_meta_data(data=value, as_obj=NodeState)
    print(f"metadata_obj: {metadata_obj}")
    print(f"readable metadata_obj: {metadata_obj.to_readable_object()}")

The running result would be like below:

.. code-block:: shell

    >>> python3 example_converter.py
    value: '["test", "Runner"]'
    metadata_obj: <class 'NodeState'>
    readable metadata_obj: {"role": "runner", "group": "sc-crawler-cluster"}

That's great! Therefore, you finish your own customized converter and verify its features could work finely, you also could apply it as option
*zk_converter* of **ZookeeperCrawler** to replace default one.

.. code-block:: python

    from smoothcrawler_cluster import ZookeeperCrawler

   zk_crawler = ZookeeperCrawler(runner=2,
                                 backup=1,
                                 ensure_initial=True,
                                 zk_hosts=_ZK_HOSTS,
                                 zk_converter=ListConverter())

That's all of how to write a customized converter by yourself. Have fun with it!
