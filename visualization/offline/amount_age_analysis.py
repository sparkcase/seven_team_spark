#!/usr/bin/python
# -*- coding:utf-8 -*-

from data_schema import DataSchema
from db_util import save_by_batch
from pyspark.sql import SparkSession


def save_to_db(par):
    data = map(lambda row: (row[0], row[1], row[2], row[3], row[4]), par)
    save_by_batch("insert into sparksql_analysis_data(age, sex, avg_value, max_value, min_value, type) VALUES (%s,%s,%s,%s,%s,'1') ", data)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("amount_age_analysis").master("local[*]").getOrCreate()

    userSchema = DataSchema.user_schema()
    userDF = spark.read.load("hdfs://master:9000/xhc/jingdong/t_user.parquet", format="parquet", schema=userSchema)
    loanSchema = DataSchema.loan_schema()
    loanDF = spark.read.load("hdfs://master:9000/xhc/jingdong/t_loan.parquet", format="parquet", schema=loanSchema)

    userDF.printSchema()
    loanDF.printSchema()

    userDF.createOrReplaceTempView("users")
    loanDF.createOrReplaceTempView("loans")

    result = spark.sql("select users.age, users.sex, avg(loans.loan_amount), max(loans.loan_amount), min(loans.loan_amount) from loans join users on loans.uid=users.uid group by users.age, users.sex")

    result.foreachPartition(lambda par: save_to_db(par))
