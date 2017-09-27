from __future__ import print_function

import boto3
import logging
import urllib2
from datetime import datetime, date, time, timedelta
import redis


import aws
import util
import redshift

print('Loading redis functions')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

POOL = redis.ConnectionPool(host='redis-poc-server.zqoahn.0001.use1.cache.amazonaws.com', port=6379, db=0)


def handler(event, context):
    function_name = context.function_name
    logging.info("function_name: " + function_name)

    r = redis.StrictRedis()

    print("mykey=", get("mykey"))

    set("mykey", "hello")
    print("mykey=", get("mykey"))

    print("done")


def get(variable_name):
    my_server = redis.Redis(connection_pool=POOL)
    response = my_server.get(variable_name)
    return response


def set(variable_name, variable_value):
    my_server = redis.Redis(connection_pool=POOL)
    my_server.set(variable_name, variable_value)