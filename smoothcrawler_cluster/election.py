"""*Election of which instance(s) is/are Runner or Backup_Runner*

In a cluster or distributed system which has high fault tolerance, it must have some instances be the backup for the
others. But how to decide which one(s) is/are **Runner** and which one(s) is/are **Backup_Runner**? This *election*
module for handling the processing.
"""

from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import List


class ElectionResult(Enum):
    """*The enum of election result*

    No matter which election strategy, they all must return the electing result outside to clear who is/are runner(s)
    and otherwise is/are loser(s).
    """

    WINNER = "Winner"
    """The winner of election for deciding who is/are runner(s)."""

    LOSER = "Loser"
    """The loser of election for deciding who is/are backup of runner(s)."""


class BaseElection(metaclass=ABCMeta):
    """*Base class of election*

    About the election running, it could have so many way to process. So here provides some abstracted functions to let
    developers extend more different strategies of election processing for more different scenarios.
    """

    @abstractmethod
    def elect(self, **kwargs) -> ElectionResult:
        """Run the election processing to verify who is/are **Runner** and otherwise is/are **Backup_Runner** finally.

        Args:
            **kwargs (dict): The parameters of this one specific election strategy processing.

        Returns:
            ElectionResult: Final election result.

        """
        pass

    @abstractmethod
    def parse_index(self, **kwargs) -> List[int]:
        """Parse all the member's identity from their name.

        Args:
            **kwargs (dict): The parameters of this one specific election strategy processing.

        Returns:
            A list of int object which are the identities from all members in cluster.

        """
        pass

    @abstractmethod
    def filter(self, **kwargs) -> List[bool]:
        """Filter all member's identity and generate winners who would be **RUNNER** in cluster, nor they would be
        **BACKUP RUNNER**.

        Args:
            **kwargs (dict): The parameters of this one specific election strategy processing.

        Returns:
            A list of bool object which means the current crawler instance is winner.

        """
        pass


class SmallerElection(BaseElection):
    """*Election by index of crawler name*

    The criteria of this election strategy is the **index** which is *the last characters in crawler's name*, e.g.,
    *sc-cluster_1*. It would get all indexes from every crawler's name and filter some of them which is/are smaller than
    the *spot* option value.

    For example, it has 5 crawlers and its name from *sc-cluster_1* to *sc-cluster_5*. If the option *spot* value is 3,
    it would mark the crawlers whose index of name is smaller than or equal to 3 as **ElectionResult.WINNER**; therefore
    , the winners of election are the crawlers *sc-cluster_1* to *sc-cluster_3*, they would be **Runner**. And the rest
    would be the **Backup_Runner**.
    """

    def elect(self, candidate: str, member: List[str], index_sep: str, spot: int) -> ElectionResult:
        """Run the election processing to verify who is/are **Runner** and otherwise is/are **Backup_Runner** finally.

        Args:
            candidate (str): The crawler instance which apply for the runner election. In generally, it is the crawler
                instance's name.
            member (list of str): All the instances which apply for the runner election.
            index_sep (str): The separation of index in crawler instance's name.
            spot (int): The amount of **Winner**, in the other words, **Runner** it could have.

        Returns:
            ElectionResult: Final election result.

        """
        # Parse the index from crawler's name
        member_indexes = self.parse_index(member=member, index_sep=index_sep)
        # Sort the indexes and filter to get the winner
        is_winner = self.filter(member_indexes, candidate=candidate, index_sep=index_sep, spot=spot)
        return ElectionResult.WINNER if True in is_winner else ElectionResult.LOSER

    def parse_index(self, member: List[str], index_sep: str) -> List[int]:
        """Parse all the member's identity from their name.

        Args:
            member (List[str]): All the members who would participate in this election. In generally, the members would
                be the value of meta-data *GroupSate.current_crawler*.
            index_sep (str): The separation which could separate the crawler instance's name and get the identity from
                it.

        Returns:
            A list of int object which are the identities from all members in cluster.

        """
        return list(map(lambda one_member: int(one_member.split(sep=index_sep)[-1]), member))

    def filter(self, member_indexes: List[int], candidate: str, index_sep: str, spot: int) -> List[bool]:
        """Filter all member's identity and generate winners who would be **RUNNER** in cluster, nor they would be
        **BACKUP RUNNER**.

        Args:
            member_indexes (List[int]): A list of int object which are the identities from all members in cluster.
            candidate (str): The current crawler instance who participates in this election.
            index_sep (str): The separation which could separate the crawler instance's name and get the identity from
                it.
            spot (int): The amount of **Winner**, in the other words, **Runner** it could have.

        Returns:
            A list of bool object which means the current crawler instance is winner.

        """
        # Sort the indexes
        sorted_list = sorted(member_indexes)
        # Filter to get the winner
        winner = sorted_list[0:spot]
        return list(map(lambda winner_index: str(winner_index) == candidate.split(index_sep)[-1], winner))
