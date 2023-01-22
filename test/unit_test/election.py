from abc import ABCMeta, abstractmethod
from smoothcrawler_cluster.election import ElectionResult, BaseElection, IndexElection
from typing import List, TypeVar, Generic
import pytest


BaseElectionType = TypeVar("BaseElectionType", bound=BaseElection)

_Test_Crawler_Name: str = "sc-crawler_1"
_Test_Loser_Crawler_Name: str = "sc-crawler_3"
_Test_Index_Sep: str = "_"
_Test_Crawlers: List[str] = ["sc-crawler_1", "sc-crawler_2", "sc-crawler_3"]
_Test_Spot: int = 2


class ElectionTestSpec(metaclass=ABCMeta):

    @abstractmethod
    def election(self) -> Generic[BaseElectionType]:
        pass


class TestIndexElection:

    @pytest.fixture(scope="function")
    def election(self) -> Generic[BaseElectionType]:
        return IndexElection()

    def test_identity(self, election: Generic[BaseElectionType]):
        assert election.identity == "", "It should be empty string in initial state."
        election.identity = _Test_Crawler_Name
        assert election.identity is not None and election.identity == _Test_Crawler_Name, \
            f"It should be assigned correct value {_Test_Crawler_Name}."

    def test_elect_if_candidate_is_winner(self, election: Generic[BaseElectionType]):
        election.identity = _Test_Crawler_Name
        election_result = election.elect(candidate=_Test_Crawler_Name,
                                         member=_Test_Crawlers,
                                         index_sep=_Test_Index_Sep,
                                         spot=_Test_Spot)
        assert isinstance(election_result, ElectionResult), \
            "The data type of election result should be *.election.ElectionResult*."
        assert election_result is ElectionResult.WINNER, \
            "It should be winner of election because of the smallest index."

    def test_elect_if_candidate_is_loser(self, election: Generic[BaseElectionType]):
        election.identity = _Test_Loser_Crawler_Name
        election_result = election.elect(candidate=_Test_Loser_Crawler_Name,
                                         member=_Test_Crawlers,
                                         index_sep=_Test_Index_Sep,
                                         spot=_Test_Spot)
        assert isinstance(election_result, ElectionResult), \
            "The data type of election result should be *.election.ElectionResult*."
        assert election_result is ElectionResult.LOSER, "It should be loser of election because of the largest index."
