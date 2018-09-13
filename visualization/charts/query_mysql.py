#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
from query_db_util import find_all


class Mysql_Query:

    def age_sex_loan_query(self, value_type="avg"):
        if value_type == "avg":
            tuples = find_all("select age, sex, avg_value from sparksql_analysis_data where type='1'")
        elif value_type == "max":
            tuples = find_all("select age, sex, max_value from sparksql_analysis_data where type='1'")
        else:
            tuples = find_all("select age, sex, min_value from sparksql_analysis_data where type='1'")
        result = self.echart_data(tuples, type="float")
        return result

    def active_after_query(self):
        tuples = find_all("select loan_start, count_value from sparksql_analysis_data where type='2'", )
        return tuples

    def no_discount_loan_query(self):
        tuples = find_all("select age, sex, count_value from sparksql_analysis_data where type='3'", )
        result = self.echart_data(tuples)
        return result

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

    def echart_data(self, tuples, type="int", groupNameFuc=None):
        cats = sorted(set(list(map(lambda t: t[0], tuples))))
        if type == "int":
            mapTuples = list(map(lambda t: (t[1], (t[0], t[2])), tuples))
        else:
            mapTuples = list(map(lambda t: (t[1], (t[0], format(float(t[2]), "0.2f"))), tuples))
        data_frame = pd.DataFrame(data=mapTuples, columns=["group", "key_value"])
        series = []
        groups = data_frame.groupby("group")
        for n, g in groups:
            data = []
            tp = list(g["key_value"])
            tp_map = {}
            for tpItem in tp:
                tp_map[tpItem[0]] = tpItem[1]
            for ca in cats:
                temp = tp_map[ca] if ca in tp_map.keys() else "-"
                data.append(temp)
            series.append((n, data))
        return (cats, series)
