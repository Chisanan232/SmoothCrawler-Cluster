import json

from kazoo.client import KazooClient

from smoothcrawler_cluster.model import Initial, RunningContent

content = RunningContent(task_id=0, url="https://www.example.com", method="GET", header={}, parameters={}, body={})
task = Initial.task(running_content=[content])

client = KazooClient(hosts="localhost:2181")
client.start()
client.set(path="smoothcrawler/node/sc-crawler_1/task", value=bytes(json.dumps(task.to_readable_object()), "utf-8"))
client.close()
