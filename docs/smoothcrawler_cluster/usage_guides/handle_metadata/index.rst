=================
Handle Meta-Data
=================

In a cluster system running, it must communicate with each others through meta-data objects to understand what happen in the cluster
so let it could judge that what thing it should do to handle it. So the management of transferring meta-data objects in cluster is
important. In generally, it has 2 methods to implement it: manage meta-data through third party application --- the most famous one
is Zookeeper, or manage meta-data by itself.

Please refer to below to get more details of it:

.. toctree::
   :maxdepth: 2
   :titlesonly:

   third_party_zookeeper
   handle_by_self

