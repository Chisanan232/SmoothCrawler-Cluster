"""*The basic attributes of crawler in SmoothCrawler-Cluster*

A crawler objects in *SmoothCrawler-Cluster* must have some basic attributes, e.g., *name*, *id_separation*, etc.
Although these attributes are necessary for each cluster crawlers, all of them are optional so the arguments of crawler
are not required. Therefore, it has a question it needs to consider: what value it should be set if it is empty value?
Absolutely, we could set it manually with any values we want. But if we want to set it more conveniently, how can it do?
For example, if we run multiple crawler instance by Docker container, do we need to set the attributes like its name for
each single containers? So this module exists for resolving this issue.
"""

from abc import ABCMeta, abstractmethod
from typing import List, Union


class BaseCrawlerAttribute(metaclass=ABCMeta):
    """*The base class of all crawler's attribute*

    Definition of crawler's attribute objects. Currently, every crawler must have one property --- *name*. Another
    property *id_separation* could be set automatically with *name*.
    """

    _instance = None

    _default_base_name: str = "sc-crawler"
    _default_id_separation: str = "_"
    _has_default: bool = True

    @property
    @abstractmethod
    def name(self) -> str:
        """:obj:`str`: Properties with both a getter and setter. This crawler instance name. It MUST be unique naming
        in cluster (the same group) for let entire crawler cluster to distinguish every one, for example, the properties
        *current_crawler*, *current_runner* and *current_backup* in meta-data **GroupState** would record by crawler
        names. This option value could be modified by Zookeeper object option *name*.
        """
        pass

    @name.setter
    @abstractmethod
    def name(self, name: str) -> None:
        pass

    @property
    @abstractmethod
    def id_separation(self) -> str:
        """:obj:`str`: Properties with both getter and setter. The string to separate the attribute *name* value to get
        identity of each one crawler instance.
        """
        pass

    @id_separation.setter
    @abstractmethod
    def id_separation(self, sep: Union[str, List[str]]) -> None:
        pass

    @property
    @abstractmethod
    def current_id(self) -> str:
        """:obj:`str`: Properties with only getter. The current identity of each one crawler instance. It MUST BE
        unique.
        """
        pass

    @property
    def has_default(self) -> bool:
        """:obj:`bool`: Properties with both getter and setter. Whether the properties *name* and *id_separation* can
        have default value or not.
        """
        return self._has_default

    @has_default.setter
    def has_default(self, has_default: bool) -> None:
        self._has_default = has_default

    def init(self, name: str, id_separation: Union[str, List[str]]) -> None:
        """Initialize the object. It has order of property setter usage. So it could use this function to initial and
        set value to properties with specific order directly.

        Args:
            name (str): The crawler instance's name. This value would be set to property *name*.
            id_separation (Union[str, List[str]]): The separation of separating crawler instance's name to get its
                identity. This value would be set to property *id_separation*.

        Returns:
            None.

        """
        self.name = name
        self.id_separation = id_separation


class NextableAttribute(BaseCrawlerAttribute):
    """*The one type of base crawler attribute with expected crawler's identity*

    This crawler attribute base class means the crawler instance's identity is expected. In the other words, it can use
    some specific way or logic to get the next identity to the new crawler instance if it needs to generate in cluster.
    """

    @property
    @abstractmethod
    def next_id(self) -> str:
        """:obj:`str`: Properties with only getter. The next one identity of crawler instance. This identity MUST be new
        and unique which doesn't be used before. This function only let you know what next one is. But it won't really
        iterate to operate.
        """
        pass

    @property
    @abstractmethod
    def iter_to_next_id(self) -> str:
        """:obj:`str`: Properties with only getter. The next one identity of crawler instance. This identity MUST be new
        and unique which doesn't be used before. This function would operate to next one, it means that if you try to
        get value by property *name*, it would turn to be the value which is equal to the return value of this property.
        """
        pass


class SerialCrawlerAttribute(NextableAttribute):
    """*The attribute let crawler's identity to be serial*

    This crawler attribute generates crawler identity as serial number like 1, 2, 3, ..., etc. This is the default
    attribute of crawler when it runs in local site directly.
    """

    _name: str = None
    _id_separation: str = None

    _id_cnt: int = 1

    @property
    def name(self) -> str:
        if self.has_default and not self._name:
            self.name = ""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        if not name:
            if not self.id_separation:
                if not self.has_default:
                    raise ValueError(
                        "The property *id_separation* value should NOT be empty when setting the *name* "
                        "property if it cannot have default value."
                    )
            self._name = f"{self._default_base_name}{self.id_separation}{self.current_id}"
        else:
            self._name = str(name)

    @property
    def id_separation(self) -> str:
        if self.has_default and not self._id_separation:
            self._id_separation = self._default_id_separation
        return self._id_separation

    @id_separation.setter
    def id_separation(self, sep: Union[str, List[str]]) -> None:
        def _chk_separation(ut_sep) -> bool:
            crawler_name_list = self.name.split(sep=ut_sep)
            try:
                int(crawler_name_list[-1])
            except ValueError:
                return False
            else:
                return True

        if not sep:
            if self.has_default:
                sep = self._default_id_separation
            else:
                raise ValueError("Argument cannot be empty when it sets *id_separation* property.")

        if not self._name:
            if not isinstance(sep, (str, list)):
                raise TypeError("*id_separation* property setter only accept 'str' or 'list[str]' type value.")
            self._id_separation = sep
            return

        if isinstance(sep, str):
            if _chk_separation(sep):
                self._id_separation = sep
                return
        elif isinstance(sep, list):
            for one_sep in sep:
                if _chk_separation(one_sep):
                    self._id_separation = one_sep
                    return
        else:
            raise TypeError("*id_separation* property setter only accept 'str' or 'list[str]' type value.")
        raise ValueError(f"This separation(s) '{sep}' cannot parse anything from the crawler name '{self.name}'.")

    @property
    def current_id(self) -> str:
        return str(self._id_cnt)

    @property
    def next_id(self) -> str:
        return str(self._id_cnt + 1)

    @property
    def iter_to_next_id(self) -> str:
        self._id_cnt = int(self.next_id)
        return str(self._id_cnt)
