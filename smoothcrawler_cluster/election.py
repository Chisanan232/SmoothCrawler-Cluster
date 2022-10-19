from abc import ABCMeta, abstractmethod


class BaseElection(metaclass=ABCMeta):

    @abstractmethod
    @property
    def identity(self) -> str:
        pass

    @abstractmethod
    @identity.setter
    def identity(self, ident: str) -> None:
        pass

    @abstractmethod
    def elect(self) -> bool:
        pass

    @abstractmethod
    def win(self) -> None:
        pass

    @abstractmethod
    def lose(self) -> None:
        pass
