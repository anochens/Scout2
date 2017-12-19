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
                info = descend(info)
            else:
                print("trying to go into %s" % arg)
                if info == [] or info == {}:
                    break # cant go any farther down

                info = apply(arg, info)

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

        info[info_id] = _ignore_keys(["users", "roles", "groups"], info[info_id])


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
    if len(record["problems"]) > 0:
        print(json.dumps(record,indent=3,sort_keys=True))
    pass

def apply(key, entity):

    if isinstance(entity, list):
        return list(map(lambda x: x[key] if (isinstance(x, dict) and key in x) else None, entity))

    # if basic
    if isinstance(entity, dict) and key in entity:
        return entity[key]

    return None

def descend(entity):

    if isinstance(entity, dict): # add all values to new array, ignoring keys
        result = []
        for k in sorted(entity.keys()):
            if isinstance(entity[k], list):
                result.extend(entity[k])
            else:
                result.append(entity[k])

        if len(result) == 1:
            return result[0]

        return result

    elif isinstance(entity, list):
        result = []
        for i in entity:
            inside = descend(i)
            result.append(inside)
        return result

    # else if is normal
    return None



def runtests():
    myobj1 = {"x":1}
    myobj2 = [{"x":1},{"x":2}]
    myobj3 = [{"x":1},{"y":2}]
    myobj4 = {"a":1,"b":2,"c":3}
    myobj5 = {"a":1,"b":{"something":"else"},"c":3}
    myobj6 = [{"something":"aaaa"},{"something":"else"},{"something":"cccc"}]

    assert (apply("x", myobj1) == 1)
    assert (apply("y", myobj1) is None)

    print(apply("x", myobj2))
    assert (apply("x", myobj2) == [1,2])

    assert (apply("x", myobj3) == [1,None])
    assert (apply("ededed",myobj1) is None)

    assert (descend({"a":1}) == 1)

    assert (descend(myobj4) == [1,2,3])
    assert (descend(myobj5) == [1,{"something":"else"},3])

    assert (apply("something",descend(myobj5)) == [None,"else",None])
    print(descend(myobj6))
    assert (descend(myobj6) == ["aaaa","else","cccc"])


def collapseToDict(entity):
    result = entity

    if isinstance(entity, list):
        result = {}
        for i in entity:
            if i != [] and i != {} and i is not None:
                if isinstance(i, dict):
                    for k in i.keys():
                        result[k] = i[k]

    return result


def main():
    runtests()
    services = [
        ["vpc","regions","id","vpcs","id","network_acls"],
        ["vpc","regions","id","vpcs","id","subnets"],
        ["vpc","regions","id","vpcs"],
        ["s3","buckets"],
        ["ec2","regions","id","vpcs","id","instances"],
        ["ec2","regions","id","vpcs","id","interfaces"],
        ["ec2","regions","id","vpcs","id","security_groups"],
        ["iam","users"],
        ["iam","roles"]
    ]
    for path_list in services:

            info = getBaseInfo(path_list)
            info = collapseToDict(info)

            if info == [] or info == {}:
                print("invalid, skipping to the next one")
                continue

            info = getInfoWithProblems(path_list[0], info)
            assert(info != [] and info != {} and info is not None)

            for i in info.values(): # no problem ignoring keys, since 'id' is already in the values dict
                #i = _cleanForES(i)
                send_to_kafka(i)


main()




