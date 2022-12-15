"""Module
TODO: Add docstring
"""

from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import List


class ElectionResult(Enum):
    """Class
    TODO: Add docstring
    """

    Winner = "Winner"
    Loser = "Loser"


class BaseElection(metaclass=ABCMeta):
    """Class
    TODO: Add docstring
    """

    _Identity: str = ""

    @property
    def identity(self) -> str:
        """Function
        TODO: Add docstring

        Returns:

        """
        return self._Identity

    @identity.setter
    def identity(self, ident: str) -> None:
        """Function
        TODO: Add docstring

        Returns:

        """
        self._Identity = ident

    @abstractmethod
    def elect(self, **kwargs) -> bool:
        """Function
        TODO: Add docstring

        Returns:

        """
        pass


class IndexElection(BaseElection):
    """Class
    TODO: Add docstring
    """

    def elect(self, candidate: str, member: List[str], index_sep: str, spot: int) -> ElectionResult:
        """Function
        TODO: Add docstring

        Returns:

        """
        member_indexs = map(lambda one_member: int(one_member.split(sep=index_sep)[-1]), member)
        sorted_list = sorted(list(member_indexs))
        winner = sorted_list[0:spot]
        is_winner = list(map(lambda winner_index: str(winner_index) in candidate, winner))
        return ElectionResult.Winner if True in is_winner else ElectionResult.Loser
