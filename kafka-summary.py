#!/usr/bin/env python

# The goal of this script is to read the output of a run of Scout2
# and forward records to a kafka topic of your choice, enhanced
# with the problem info reported by Scout2.

import json

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


def getBaseInfo(service, subservice):
    try:
        info = getOrLoadData()["services"][service][subservice]
        return info
    except KeyError:
        print("No %s %s info to get info for." % (service,subservice))
        return {}


# iterate through the problems and enhance info record with an array of their problems
def getInfoWithProblems(service, info):
    data = getOrLoadData()

    print("Enhancing %d records for service %s" % (len(info), service))

    problems = data["services"][service]["findings"]

    for info_id in info.keys():
        if not "problems" in info[info_id]:
            info[info_id]["problems"] = []

        for problem_k in problems.keys():
            problem = problems[problem_k]

            if info_id in ",".join(problem["items"]): #get around the wierd prefix and suffix

                info[info_id]["problems"].append(_select_keys(["description","level"], problem))

        info[info_id] = _ignore_keys(["users","roles", "groups"], info[info_id])

        # turn nested dictionaries into json pretty-strings
        # to not error in ElasticSearch
        for k in info[info_id].keys():
            if isinstance(info[info_id][k], dict) or\
                    (isinstance(info[info_id][k], list) and len(info[info_id][k]) > 0 and isinstance(info[info_id][k][0], dict)):
                info[info_id][k] = json.dumps(info[info_id][k], indent=3)


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
    print(json.dumps(record,indent=3))
    pass


def main():

    services = {"s3":["buckets"]}
    services = {"iam":["policies","groups","roles","users"]}
    for service in services.keys():
        for subservice in services[service]:
            info = getBaseInfo(service, subservice)
            info = getInfoWithProblems(service, info)


            for i in info.values(): # no problem ignoring keys, since 'id' is already in the values dict
                send_to_kafka(i)


main()

