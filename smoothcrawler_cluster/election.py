from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import List


class ElectionResult(Enum):

    Winner = "Winner"
    Loser = "Loser"


class BaseElection(metaclass=ABCMeta):

    _Identity: str = ""

    @property
    def identity(self) -> str:
        return self._Identity

    @identity.setter
    def identity(self, ident: str) -> None:
        self._Identity = ident

    @abstractmethod
    def elect(self, **kwargs) -> bool:
        pass


class IndexElection(BaseElection):

    def elect(self, candidate: str, member: List[str], index_sep: str, spot: int) -> ElectionResult:
        _member_indexs = map(lambda one_member: int(one_member.split(sep=index_sep)[-1]), member)
        _sorted_list = sorted(list(_member_indexs))
        _winner = _sorted_list[0:spot]
        _is_winner = list(map(lambda winner_index: str(winner_index) in candidate, _winner))
        if True in _is_winner:
            return ElectionResult.Winner
        else:
            return ElectionResult.Loser
