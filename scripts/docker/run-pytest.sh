#!/usr/bin/env bash

##########################################################################################
#
# Target:
# For other tool like *Docker* or *tox* to use.
#
# Description:
# It does 2 things: run script for getting testing items and run the testing via tool *pytest*.
# This bash file must to receive a argument *testing_type* which is the key condition to let
# script runs unit test or integration test.
#
# Allowable argument:
# * unit-test: Get and run unit test.
# * integration-test: Get and run integration test.
#
##########################################################################################

set -ex
testing_type=$1
echo "⚙️ It would run the " +  testing_type + " of the Python package SmoothCrawler-Cluster."

if echo "$testing_type" | grep -q "unit-test";
then
    test_path=$(bash ./scripts/ci/get-unit-test-paths.sh windows | sed "s/\"//g" | sed 's/^//')
elif echo "$testing_type" | grep -q "integration-test";
then
    test_path=$(bash ./scripts/ci/get-integration-test-paths.sh windows | sed "s/\"//g" | sed 's/^//')
else
  echo "❌ Got error when it tried to get testing items... 😱"
  exit 1
fi

pytest $test_path
