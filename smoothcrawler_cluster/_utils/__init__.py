"""Module docstring
TODO: Need to add document here
"""

from typing import Union, Type, TypeVar, Generic

from .converter import BaseConverter as _BaseConverter, JsonStrConverter, TaskContentDataUtils
from .zookeeper import _BaseZookeeperNode, _BaseZookeeperClient, ZookeeperNode, ZookeeperRecipe, ZookeeperClient
from ..model import Empty, GroupState, NodeState, Task, Heartbeat
from ..model.metadata import _BaseMetaData


_BaseMetaDataType = TypeVar("_BaseMetaDataType", bound=_BaseMetaData)


def parse_timer(timer: str) -> Union[int, float]:
    """
    TODO: Function Docstring
    Args:
        timer:

    Returns:

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
    """Class Docstring
    TODO: Need to add document
    """

    _zookeeper_client: ZookeeperClient = None
    _zookeeper_data_converter: _BaseConverter = None

    _default_zookeeper_hosts: str = "localhost:2181"

    def __init__(self, converter: _BaseConverter, client: ZookeeperClient = None):
        if client is None:
            client = ZookeeperClient(hosts=self._default_zookeeper_hosts)
        self._zookeeper_client = client
        self._zookeeper_data_converter = converter

    def get_metadata_from_zookeeper(
            self,
            path: str,
            as_obj: Type[_BaseMetaDataType],
            must_has_data: bool = True,
    ) -> Generic[_BaseMetaDataType]:
        """
        TODO: Add Function docstring
        Args:
            path:
            as_obj:
            must_has_data:

        Returns:

        """
        value = self._zookeeper_client.get_value_from_node(path=path)
        if value:
            state = self._zookeeper_data_converter.deserialize_meta_data(data=value, as_obj=as_obj)
            return state
        else:
            if must_has_data is True:
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
        """
        TODO: Add Function docstring
        Args:
            path:
            as_obj:
            must_has_data:

        Returns:

        """
        metadata_str = self._zookeeper_data_converter.serialize_meta_data(obj=metadata)
        if create_node is True:
            self._zookeeper_client.create_node(path=path, value=metadata_str)
        else:
            self._zookeeper_client.set_value_to_node(path=path, value=metadata_str)
