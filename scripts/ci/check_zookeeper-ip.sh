#!/usr/bin/env bash

#set -ex

zookeeper_docker_container_name=$1
if [ "$zookeeper_docker_container_name" == "" ]; then
    zookeeper_docker_container_name="zookeeper"
fi
zookeeper_ip=$(/usr/bin/docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$zookeeper_docker_container_name")
if [ "$zookeeper_ip" == "172.18.0.2" ]; then
    echo "✅📇 It's correct and expected IP address of Zookeeper service."
    exit 0
else
    echo "❌ It's incorrect and unexpected IP address of Zookeeper service, please let DevOps engineer to check it."
    exit 1
fi
