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


def getBaseInfo(args):
    try:
        info = getOrLoadData()["services"]
        for arg in args:
            print("arg is %s" % arg)

            # need to handle 'id' specially
            if arg == 'id':
                print('descending on an id')
                info = _descend(info)
            else:
                print("trying to go into %s" % arg)
                if info == [] or info == {}:
                    break # cant go any farther down

                info = info[arg]

            print("went into %s" % arg)
        return info
    except KeyError:
        print("No  info to get info for path %s." % (str(args)))
        return {}


def _cleanForES(value):
    # turn nested dictionaries into json pretty-strings
    # to not error in ElasticSearch
    for k in value.keys():
        if isinstance(value[k], dict) or \
                (isinstance(value[k], list) and len(value[k]) > 0 and isinstance(value[k][0], dict)):
            value[k] = json.dumps(value[k], indent=3)

    return value


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
        #info[info_id] = _cleanForES(info[info_id])

    return info

# descend into an array without the keys, similar to jq's .[] functionality
def _descend(mydict):
    result = []
    for v in mydict.values():
        if isinstance(v, list):
            result.append(v)

    return result

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
    if len(record["problems"]) > 0:
        print(json.dumps(record,indent=3,sort_keys=True))
    pass


def main():

    services = [["s3","buckets"]]
    services = [["iam","users"]]
    #services = [["ec2","regions","id","vpcs","id","network_interfaces"]]

    for path_list in services:

            info = getBaseInfo(path_list)

            if info == [] or info == {}:
                print("invalid, skipping to the next one")
                continue

            info = getInfoWithProblems(path_list[0], info)


            for i in info.values(): # no problem ignoring keys, since 'id' is already in the values dict
                send_to_kafka(i)


main()

