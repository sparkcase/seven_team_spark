#!/usr/bin/python
# -*- coding:utf-8 -*-

from data_schema import DataSchema
from db_util import save_by_batch
from pyspark.sql import SparkSession


def save_to_db(par):
    data = map(lambda row: (row[0], row[1]), par)
    save_by_batch("insert into sparksql_analysis_data(loan_start, count_value, type) VALUES (%s,%s,'2') ", data)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("active_loan_analysis").master("local[*]").getOrCreate()

    userSchema = DataSchema.user_schema()
    userDF = spark.read.load("hdfs://master:9000/xhc/jingdong/t_user.csv", format="csv", schema=userSchema)
    loanSchema = DataSchema.loan_schema()
    loanDF = spark.read.load("hdfs://master:9000/xhc/jingdong/t_loan.csv", format="csv", schema=loanSchema)

    userDF.printSchema()
    loanDF.printSchema()

    userDF.createOrReplaceTempView("users")
    loanDF.createOrReplaceTempView("loans")

    result = spark.sql("select '3day' as after, count(distinct loans.uid) as n_user from loans join users on loans.uid=users.uid where users.active_date is not null and loans.loan_time is not null and datediff(to_date(loans.loan_time), to_date(users.active_date))<=3 "
                       "union select '7day' as after, count(distinct loans.uid) as n_user from loans join users on loans.uid=users.uid where users.active_date is not null and loans.loan_time is not null and datediff(to_date(loans.loan_time), to_date(users.active_date))<=7 "
                       "union select '1month' as after, count(distinct loans.uid) as n_user from loans join users on loans.uid=users.uid where users.active_date is not null and loans.loan_time is not null and to_date(loans.loan_time)<=add_months(users.active_date, 1) "
                       "union select '2month' as after, count(distinct loans.uid) as n_user from loans join users on loans.uid=users.uid where users.active_date is not null and loans.loan_time is not null and to_date(loans.loan_time)<=add_months(users.active_date, 2) "
                       "union select '3month' as after, count(distinct loans.uid) as n_user from loans join users on loans.uid=users.uid where users.active_date is not null and loans.loan_time is not null and to_date(loans.loan_time)<=add_months(users.active_date, 3)")

    result.foreachPartition(lambda par: save_to_db(par))
