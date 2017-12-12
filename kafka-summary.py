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


def getBaseInfo(service):
    try:
        return getOrLoadData()["services"][service]["buckets"]
    except KeyError:
        print("No %s info to get info for." % service)
        return {}


# iterate through the problems and enhance info record with an array of their problems
def getInfoWithProblems(service, info):
    data = getOrLoadData()

    print("Enhancing %d info for service %s" % (len(info), service))

    problems = data["services"][service]["findings"]

    for info_id in info.keys():
        if not "problems" in info[info_id]:
            info[info_id]["problems"] = []

        for problem_k in problems.keys():
            problem = problems[problem_k]

            if info_id in ",".join(problem["items"]): #get around the wierd prefix and suffix

                info[info_id]["problems"].append(_select_keys(["description","level"], problem))

        info[info_id] = _ignore_keys(["users","roles", "groups"], info[info_id])
    return info


# select only the keys that are wanted (not the ones with global scope)
def _select_keys(wanted, mydict):
    p = {}
    for k in mydict:
        if k in wanted:
            p[k] = mydict[k]

    return p

def _ignore_keys(unwanted, mydict):
    p = {}
    for k in mydict:
        if k not in unwanted:
            p[k] = mydict[k]

    return p

# prepare and send a given record to kafka (insert @timestamp, etc.)
def send_to_kafka(record):
    # TODO send to kafka
    pprint(record)
    pass


def main():
    info = getBaseInfo("s3")
    info = getInfoWithProblems("s3", info)

    for i in info.values(): # no problem ignoring keys, since 'id' is already in the values dict
        send_to_kafka(i)


main()

