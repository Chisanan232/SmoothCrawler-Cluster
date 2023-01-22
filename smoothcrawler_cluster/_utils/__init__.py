"""*Sub-pacakge of util features*

In design of distributed system or cluster, it always depends on some third party applications like message queue system
(e.g., Kafka) or distributed system management, e.g., Zookeeper. So it has implementations is relative with that, but in
generally, it won't provide them to outside to use or extend. That's the reason why this inner sub-package exists.

Here are some util functions like Zookeeper client or data processing of serializing or deserializing. Below functions
or objects be encapsulated again to be more convenience and readable in usage.
"""

from typing import Union, Optional, Type, TypeVar, Generic

from .converter import BaseConverter, JsonStrConverter, TaskContentDataUtils
from .zookeeper import _BaseZookeeperNode, _BaseZookeeperClient, ZookeeperNode, ZookeeperRecipe, ZookeeperClient
from ..model import Empty, GroupState, NodeState, Task, Heartbeat
from ..model.metadata import _BaseMetaData


_BaseMetaDataType = TypeVar("_BaseMetaDataType", bound=_BaseMetaData)


def parse_timer(timer: str) -> Union[int, float]:
    """Parse the sleep time value to be an int or float type value.

    Args:
        timer (str): The timer value to parse. Its time unit could be hour, minute or second.

    Returns:
        Union[int, float]: How many seconds it equals to. And it would return as float type value if it has decimal.

    Raises:
        ValueError (not number format): Invalid value which is NOT number format value so that it cannot be parsed.
        ValueError (incorrect last character): The last character is NOT correct so that it couldn't judge its time unit
            is hour, minute or second.

    """
    timer_val = timer[:-1]
    try:
        if "." in timer_val:
            time = float(timer_val)
        else:
            time = int(timer_val)
    except ValueError as e:
        raise ValueError(f"Invalid value {timer_val}. It should be an integer format value.") from e

    time_unit = timer[-1]
    if time_unit == "s":
        sleep_time = time
    elif time_unit == "m":
        sleep_time = time * 60
    elif time_unit == "h":
        sleep_time = time * 60 * 60
    else:
        raise ValueError("It only supports 's' (seconds), 'm' (minutes) or 'h' (hours) setting value.")
    return sleep_time


class MetaDataUtil:
    """*Encapsulate the util features*

    Integrate the util features, includes Zookeeper client and data processing of converting as another object which is
    more convenience and readable to use.
    """

    _zookeeper_client: ZookeeperClient = None
    _zookeeper_data_converter: BaseConverter = None

    _default_zookeeper_hosts: str = "localhost:2181"

    def __init__(self, converter: BaseConverter, client: Optional[ZookeeperClient] = None):
        """

        Args:
            converter (BaseConverter): The converter to do data processing.
            client (Optional[ZookeeperClient]): The Zookeeper client.
        """
        if not client:
            client = ZookeeperClient(hosts=self._default_zookeeper_hosts)
        self._zookeeper_client = client
        self._zookeeper_data_converter = converter

    def get_metadata_from_zookeeper(
            self,
            path: str,
            as_obj: Type[_BaseMetaDataType],
            must_has_data: bool = True,
    ) -> Generic[_BaseMetaDataType]:
        """Get meta-data from Zookeeper.

        Args:
            path (str): The node path.
            as_obj (Type[_BaseMetaDataType]): The target object it deserializes value to be.
            must_has_data (bool): If it's True, it must return an object as the type it set by argument *as_obj*. In the
                other words, it would return an empty meta-data object if it gets None from Zookeeper. If it's False, it
                would return None if it gets nothing from Zookeeper. Default value is True.

        Returns:
            Generic[_BaseMetaDataType]: The meta-data which has been deserialized as _BaseMetaData type instance from
                Zookeeper.

        Raises:
            TypeError: If the value type of argument *as_obj* DOES NOT one of these 4 types meta-data:
                :ref:`GroupState <MetaData_GroupState>`, :ref:`NodeState <MetaData_NodeState>`,
                :ref:`Task <MetaData_Task>` and :ref:`Heartbeat <MetaData_Heartbeat>`.

        """
        value = self._zookeeper_client.get_value_from_node(path=path)
        if value:
            state = self._zookeeper_data_converter.deserialize_meta_data(data=value, as_obj=as_obj)
            return state
        else:
            if must_has_data:
                if issubclass(as_obj, GroupState):
                    return Empty.group_state()
                elif issubclass(as_obj, NodeState):
                    return Empty.node_state()
                elif issubclass(as_obj, Task):
                    return Empty.task()
                elif issubclass(as_obj, Heartbeat):
                    return Empty.heartbeat()
                else:
                    raise TypeError(f"It doesn't support deserialize data as type '{as_obj}' recently.")
            else:
                return None

    def set_metadata_to_zookeeper(
            self,
            path: str,
            metadata: Generic[_BaseMetaDataType],
            create_node: bool = False,
    ) -> None:
        """Set value of meta-data to node by path in Zookeeper.

        Args:
            path (str): The node path.
            metadata (Generic[_BaseMetaDataType]): The meta-data object to set to Zookeeper.
            create_node (bool): If it's True, it would create the node with value. Nor it would set the value to node
                directly. Default value is False.

        Returns:
            None

        """
        metadata_str = self._zookeeper_data_converter.serialize_meta_data(obj=metadata)
        if create_node:
            self._zookeeper_client.create_node(path=path, value=metadata_str)
        else:
            self._zookeeper_client.set_value_to_node(path=path, value=metadata_str)
