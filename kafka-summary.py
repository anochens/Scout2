#!/usr/bin/env python

# The goal of this script is to read the output of a run of Scout2
# and forward records to a kafka topic of your choice, enhanced
# with the problem info reported by Scout2.

import json
from pprint import pprint

data = {}

def getLocation():
    return "./scout2-report/inc-awsconfig/aws_config.js"


def loadData():
    global data
    with open(getLocation()) as f:
        next(f) # skip the bad first line
        d = next(f)

        data = json.loads(d)


def getOrLoadData():
    global data
    if not data:
        loadData()

    return data


def getBaseInfo():
    try:
        return getOrLoadData()["services"]["s3"]["buckets"]
    except KeyError:
        print "No s3 buckets to get info for."
        return {}


# iterate through the problems and enhance bucket record with an array of their problems
def getInfoWithProblems(buckets):
    data = getOrLoadData()

    print "Enhancing %d buckets" % len(buckets)

    problems = data["services"]["s3"]["findings"]

    for bucket_id in buckets.keys():
        if not "problems" in buckets[bucket_id]:
            buckets[bucket_id]["problems"] = []

        for problem_k in problems.keys():
            problem = problems[problem_k]

            if bucket_id in ",".join(problem["items"]): #get around the wierd prefix and suffix

                buckets[bucket_id]["problems"].append(_select(problem, ["description","level"]))
    return buckets


# select only the keys that are wanted (not the ones with global scope)
def _select(mydict, wanted):
    p = {}
    for k in mydict:
        if k in wanted:
            p[k] = mydict[k]

    return p


# prepare and send a given record to kafka (insert @timestamp, etc.)
def send_to_kafka(record):
    # TODO send to kafka
    pprint(record)
    pass


def main():
    buckets = getBaseInfo()
    buckets = getInfoWithProblems(buckets)

    for bucket in buckets.values(): # no problem ignoring keys, since 'id' is already in the values dict
        send_to_kafka(bucket)

main()

