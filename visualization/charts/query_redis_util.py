#!/usr/bin/python
# -*- coding:utf-8 -*-

import sys

import redis

sys.path.append("../")
from db_config import redis_config


class RedisConn:
    def __init__(self):
        self.pool = redis.ConnectionPool(host=redis_config["host"], port=redis_config["port"])

    def get_conn(self):
        return redis.Redis(connection_pool=self.pool)

    def close_conn(self, conn):
        print("close")

    @staticmethod
    def get_redis_conn_instance():
        return RedisConn()


def get_by_key_pattern(pat):
    rc = RedisConn.get_redis_conn_instance()
    r = rc.get_conn()
    keys = r.keys(pattern=pat)
    result = []
    for k in keys:
        result.append((k.decode(), r.get(k).decode(),))
    return result
