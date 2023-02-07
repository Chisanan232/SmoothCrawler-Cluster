"""*crawler features*

The crawler feature in *SmoothCrawler-Cluster* is complex. For to be convenience and maintaining to manage it, it
divides the crawler features into 4 parts as different single modules:

* The essential crawler (*smoothcrawler_cluster.crawler.crawlers*)
    The main body of crawler, so all the object like **XXXCrawler** would be here.

* Workflow for crawler with different roles (*smoothcrawler_cluster.crawler.workflow*)
    The details about what process it would do with its crawler role would be implemented in this section. However,
    crawler should NOT and does NOT need to use this module or its object directly, it has **dispatcher** module to
    help crawler to get the workflow it needs.

* Dispatcher of workflow for crawler with different roles (*smoothcrawler_cluster.crawler.dispatcher*)
    The dispatcher to dispatch the correct workflow crawler needs with its crawler role. So crawler only calls
    dispatcher's function easily and uses the return object directly.

* The adapter of some features which may need to use in cluster (*smoothcrawler_cluster.crawler.adapter*)
    Integrate some feature, e.g., distributed lock, into one object as adapter to let workflow could use it in
    implementation and crawler also could assign some objects it needs.

"""

from .crawlers import ZookeeperCrawler
