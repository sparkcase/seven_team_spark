#!/usr/bin/env python
# -*- coding: utf-8 -*-

from query_redis_util import get_by_key_pattern


class Redis_Query:

    def click_query(self):
        tuples = get_by_key_pattern("click[0-9]*")
        return tuples

    def order_query(self):
        tuples = get_by_key_pattern("buy[0-9]*")
        return tuples

    def get_keys(self, tuples):
        keys = []
        for tuple in tuples:
            keys.append(tuple[0])
        return keys

    def get_values(self, tuples):
        values = []
        for tuple in tuples:
            values.append(tuple[1])
        return values
