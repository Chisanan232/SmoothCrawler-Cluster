=================
Leader (Master)
=================

It is the crawler instance who gives order to **Follower** to do something. In *SmoothCrawler-Cluster*, although it may have multiple
**Leader** instance, only one instance has responsible for being **Leader**, we call it **Primary Leader**, and the rest would be the
backup one to standby for being **Primary Leader**, we call it **Secondary Leader**.

Responsibility
===============

Here it would explain the details what things it should do as each different roles.

Primary leader
---------------

If it has only one crawler instance as **Leader**, it would be **Primary Leader** and it is responsible of giving order to **Follower**
and monitoring them. But if it has multiple crawler instances as **Leader**, the **Primary Leader** instance only need to give order to
**Follower**.

.. note::

    If it has multiple crawler instances as **Leader**, it also uses election to decides to who is the **Primary Leader**.

Secondary Leader
-----------------

It has **Secondary Leader** if it has multiple **Leader** crawler instances; otherwise, it doesn't. **Secondary Leader** is responsible of
2 things: monitor all **Follower** instances and stand by for being **Primary Leader**.
