#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from pyhive import presto

PRESTO_SERVER = {'host': 'master', 'port': 8087, 'catalog': 'hive', 'schema': 'default'}
PRICE_PERDAY_QUERY = "select t_user.age, t_order.buy_time, sum(t_order.price * t_order.qty) as total_price from t_user join t_order on t_user.uid=t_order.uid where t_order.qty>0 group by t_user.age,t_order.buy_time order by t_order.buy_time limit 200"
GENDER_LOAN_DAY_QUERY = "select t_user.sex, date_trunc('day', load_time) loan_time, sum(t_loan.loan_amount) loan_amount from t_user join t_loan on t_user.uid=t_loan.uid where load_time is not NULL group by t_user.sex, date_trunc('day', load_time) order by date_trunc('day', load_time) limit 100"
GENDER_LOAN_WEEK_QUERY = "select t_user.sex, date_trunc('week', load_time) loan_time, sum(t_loan.loan_amount) loan_amount from t_user join t_loan on t_user.uid=t_loan.uid where load_time is not NULL group by t_user.sex, date_trunc('week', load_time) order by date_trunc('week', load_time) limit 100"
GENDER_LOAN_MONTH_QUERY = "select t_user.sex, date_trunc('month', load_time) loan_time, sum(t_loan.loan_amount) loan_amount from t_user join t_loan on t_user.uid=t_loan.uid where load_time is not NULL group by t_user.sex, date_trunc('month', load_time) order by date_trunc('month', load_time) limit 100"

class Presto_Query:
    def total_price_perday(self):
        conn = presto.connect(**PRESTO_SERVER)
        cur = conn.cursor()
        cur.execute(PRICE_PERDAY_QUERY)
        tuples = cur.fetchall()
        result = self.echartData(tuples)
        return result

    def gender_loan_query(self, peroid=None):
        conn = presto.connect(**PRESTO_SERVER)
        sql = GENDER_LOAN_DAY_QUERY if (peroid is None) or (peroid == "day") else (GENDER_LOAN_WEEK_QUERY if peroid == "week" else GENDER_LOAN_MONTH_QUERY)
        cur = conn.cursor()
        cur.execute(sql)
        tuples = cur.fetchall()
        result = self.echartData(tuples)
        return result

    def echartData(self, tuples, groupNameFuc=None):
        times = sorted(set(list(map(lambda t: t[1][:11], tuples))))
        mapTuples = list(map(lambda t: (t[0], (t[1][:11], format(float(t[2]), "0.2f"))), tuples))

        dataFrame = pd.DataFrame(data=mapTuples, columns=["group", "time_price"])
        series = []
        groups = dataFrame.groupby("group")
        for n, g in groups:
            data = []
            tp = list(g["time_price"])
            tpMap = {}
            for tpItem in tp:
                tpMap[tpItem[0]] = tpItem[1]
            for ti in times:
                temp = tpMap[ti] if ti in tpMap.keys() else "-"
                data.append(temp)
            series.append((n, data))
        return (times, series)