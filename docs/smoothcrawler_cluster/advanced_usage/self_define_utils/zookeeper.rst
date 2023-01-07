======================================
How to implement new Zookeeper client
======================================

In *SmoothCrawler-Cluster* package, it implements Zookeeper client feature with `Kazoo`_. That's the reason why its dependency
has *kazoo*. However, it you have other customized requirement so that it must to implement another new one, here would
teach you step by step how to implement a your own customized Zookeeper client feature.

.. _Kazoo: https://kazoo.readthedocs.io/en/latest/#

About customizing a new Zookeeper client API, we could import the base class **_BaseZookeeperClient** from
*smoothcrawler_cluster._utils.zookeeper*:

.. code-block:: python

    from smoothcrawler_cluster._utils.zookeeper import _BaseZookeeperClient

    class NewZKClient(_BaseZookeeperClient):
        # all function implementations of Zookeeper client

After extending the base class, let's start to demonstrate how to implement each functions you need.

By the way, it would demonstrate how to implement through explaining what thing you need to do with a non-existed package
as example because I cannot find another mature Zookeeper client in Python.

.. note::

    If you're an old hand of Python developer, you should observe that this base class is a protected class. Why? Why it's
    not public class? The reason is: I found that there is ONLY ONE Python package be suggested and be mature in mostly
    scenarios. And the only one package is *kazoo*. So it a little bit doesn't suggest to re-implement it because there is
    no any package is a better choice than *kazoo*. However, it still could re-implement to extend feature it if you really
    have another issues or scenarios must to resolve.

Implement CRUD
---------------

One of Zookeeper important features is data storage. You could save some data in Zookeeper and let it manage it. So we must
to implement the CRUD features if we want to customize new Zookeeper client API.

Create new node
^^^^^^^^^^^^^^^^^

**Create a node** with ``path`` and also includes ``value`` if it needs.

.. code-block:: python

    def create_node(self, path: str, value: Union[str, bytes] = None) -> str:
        if not value:
            return example_zk_lib.create(path=path)

        if isinstance(value, str):
            return example_zk_lib.create(path=path, value=bytes(value, "utf-8"))
        elif isinstance(value, bytes):
            return example_zk_lib.create(path=path, value=value)
        else:
            raise TypeError("It only supports *str* or *bytes* data types.")

Please pay attention of the example implementation, it has 2 major features:

* Check whether the value of option ``value`` is *None* or not. It would only create the node but doesn't assign value to it if ``value`` is *None*.
* Convert the ``value`` type as *str* or *bytes*.

Above features could be adjusted, even be removed by developer's concerns.

Get value from node
^^^^^^^^^^^^^^^^^^^^

**Get the value from node** with ``path``.

.. code-block:: python

    def get_node(self, path: str) -> Generic[_BaseZookeeperNodeType]:
        data = example_zk_lib.get(path=path)

        zk_path = ZookeeperNode()
        zk_path.path = path
        zk_path.value = data.decode("utf-8")

        return zk_path

In this function, please remember that it MUST to convert the data to a **ZookeeperNode** object and return it.

Update value at node
^^^^^^^^^^^^^^^^^^^^^^

**Set value to node** with ``path``.

.. code-block:: python

    def set_value_to_node(self, path: str, value: Union[str, bytes]) -> None:
        if isinstance(value, str):
            example_zk_lib.set(path=path, value=value.encode("utf-8"))
        elif isinstance(value, bytes):
            example_zk_lib.set(path=path, value=value)
        else:
            raise TypeError("It only supports *str* or *bytes* data types.")

Delete value at node
^^^^^^^^^^^^^^^^^^^^^^

**Delete the node** with ``path``.

.. code-block:: python

    def delete_node(self, path: str) -> bool:
        return example_zk_lib.delete(path=path)

Common functions
------------------

Apart from the **CRUD** features, it must has some common functions to let usage could be more convenience.

Check whether the node exist or not
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Checking whether the node exist or not with ``path``.

.. code-block:: python

    def exist_node(self, path: str) -> Optional[Any]:
        return example_zk_lib.exists(path=path)

Generate distributed lock
^^^^^^^^^^^^^^^^^^^^^^^^^^^

In Zookeeper usage, if you want to operate one specific node as atomic operating, it would needs to have function to generate
distributed lock.

.. code-block:: python

    def restrict(
            self,
            path: str,
            restrict: ZookeeperRecipe,
            identifier: str,
            max_leases: int = None,
    ) -> Union[ReadLock, WriteLock, Semaphore]:
        restrict_obj = getattr(example_zk_lib, str(restrict.value))
        if max_leases:
            restrict = restrict_obj(path, identifier, max_leases)
        else:
            restrict = restrict_obj(path, identifier)
        return restrict

Here implementation would be a little bit special: it would try to get the lock object via native function ``getattr`` and
instantiate it. So here you must to ensure that the lock object could be import as below:

.. code-block:: python

    from example_zk_lib import ReadLock, WriteLock, Semaphore

If it cannot, than you should modify the object as the import path where the lock object could be imported.

You also have another thing need to confirm: The value of enum object **ZookeeperRecipe** (option ``restrict``). It would
try to get lock object from the object ``example_zk_lib``, and the lock object it try to get would be assigned as **ZookeeperRecipe**.
So we should ensure that the values of enum object is valid, in the other words, the values should be same as the lock object
we want to use, i.e., it should be have **ReadLock**, **WriteLock** and **Semaphore** in object ``example_zk_lib``, or we
should also re-implement this enum object as below:

.. code-block:: python

    class NewZKRecipe(ZookeeperRecipe):
        READ_LOCK: str = "NewReadLock"
        WRITE_LOCK: str = "NewWriteLock"
        SEMAPHORE: str = "NewSemaphore"

And change the data type hint of option ``restrict`` as the new enum object:

.. code-block:: python

    def restrict(
            self,
            path: str,
            restrict: NewZKRecipe,
            identifier: str,
            max_leases: int = None,
    ) -> Union[ReadLock, WriteLock, Semaphore]:
        # Your implementation

That's all what you should to do if you want to customize Zookeeper client API. But once again, the mature Zookeeper client
API in Python is *kazoo* only. So it won't suggest you to re-implement it if you have other better choices.
