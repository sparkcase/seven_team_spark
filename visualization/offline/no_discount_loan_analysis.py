#!/usr/bin/python
# -*- coding:utf-8 -*-

from data_schema import DataSchema
from db_util import save_by_batch
from pyspark.sql import SparkSession


def save_to_db(par):
    data = map(lambda row: (row[0], row[1], row[2]), par)
    save_by_batch("insert into sparksql_analysis_data(age, sex, count_value, type) VALUES (%s,%s,%s,'3') ", data)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("no_discount_loan_analysis").master("local[*]").getOrCreate()

    userSchema = DataSchema.user_schema()
    userDF = spark.read.load("hdfs://master:9000/xhc/jingdong/t_user.parquet", format="parquet", schema=userSchema)
    loanSchema = DataSchema.loan_schema()
    loanDF = spark.read.load("hdfs://master:9000/xhc/jingdong/t_loan.parquet", format="parquet", schema=loanSchema)
    orderSchema = DataSchema.order_schema()
    orderDF = spark.read.load("hdfs://master:9000/xhc/jingdong/t_order.parquet", format="parquet", schema=orderSchema)

    userDF.printSchema()
    loanDF.printSchema()
    orderDF.printSchema()

    userDF.createOrReplaceTempView("users")
    loanDF.createOrReplaceTempView("loans")
    orderDF.createOrReplaceTempView("orders")


    result = spark.sql("select users.age, users.sex, count(*) from users "
                       "where users.uid not in (select distinct orders.uid from orders where orders.uid is not null and orders.discount>0) and "
                       "users.uid not in (select distinct loans.uid from loans where loans.uid is not null and loans.loan_amount>0) "
                       "group by users.age, users.sex")

    result.foreachPartition(lambda par: save_to_db(par))
