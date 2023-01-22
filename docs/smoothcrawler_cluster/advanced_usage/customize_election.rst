==============================
How to customize new Election
==============================

*Election* is a very important processing in *SmoothCrawler-Cluster* because it would decide which crawler instance(s) is/are
**Runner** and the rest is/are **Backup Runner**. However, in current design, it would only run once --- at the first time to
start to run the crawler cluster. So it would control how the cluster should run in the beginning.

But for cluster system development, it may face many different scenarios in usage. So it also provides base class to let
developers to follow and extend the election feature for their own self usage scenarios.

.. note::

    About this component, it would have another one to run with it --- **Naming** which could control how to name the crawler
    instance with different condition or usage scenarios. It would be new feature in the next version.

The base class could be import from module *smoothcrawler_cluster.election*:

.. code-block:: python

    from smoothcrawler_cluster.election import BaseElection

    class ExampleElection(BaseElection):
        # all function implementations of election

Let's demonstrate a customized election which would convert the crawler name to integer and filter the bigger one(s) to be
the **Runner**.


Implement election features
-----------------------------

For implementing a new election object, it only needs implement one function *elect*:

.. code-block:: python

    def elect(self, candidate: str, member: List[str], spot: int) -> ElectionResult:
        # Convert the name to int type value
        member_indexs = map(lambda one_member: int(one_member), member)
        # Sort them
        sorted_list = sorted(list(member_indexs))
        # Get the number with target amount
        winner = sorted_list[0:spot]
        # Check whether the winner list includes crawler name or not
        is_winner = list(map(lambda winner_index: str(winner_index) == candidate, winner))
        return ElectionResult.WINNER if True in is_winner else ElectionResult.LOSER

Above logic is very easy. It would convert all the crawler name to int type value and sort them. And after it has the sorted
list, it could filter the elements to get a winner list and check that whether the current crawler name be included in the
winner list or not.


Verify the election effect
---------------------------

Here gives it a simple data for testing:

.. code-block:: python

    all_members = ["1", "2", "3"]
    crawler_name = "1"
    runner_amount = 2

``all_members`` is the all members who join to this election processing.

``crawler_name`` is the fake crawler name for testing as current instance.

In generally, we would pass the value of all **Runner** amount to it so that it could run the election to filter and get
how many the winner we need. So we will need the value ``runner_amount``.

.. code-block:: python

    from smoothcrawler_cluster.election import ElectionResult

    ex_ele_strategy = ExampleElection()
    result = ex_ele_strategy.election(candidate=all_members,
                                      member=crawler_name,
                                      spot=runner_amount)
    print(result is ElectionResult.WINNER)

The final log message should print *True* because the sorted list of crawler names is ``[1, 2, 3]`` and the **WINNER** amount
we need is 2, the ``"1"`` would be included in the first 2 elements. In the other words, it would still be **WINNER** if you
change the crawler name as ``"2"``. But it would turn to be **LOSER** because it doesn't be included in first 2 elements and
it would be **Backup Runner** finally.
