#!/usr/bin/python
# -*- coding:utf-8 -*-

from pyspark.sql.types import *


class DataSchema:

    @staticmethod
    def user_schema():
        schema_string = "uid,age,sex,active_date,limit"
        field_strs = schema_string.split(",")
        fields = []
        fields.append(StructField(field_strs[0], StringType(), False))
        fields.append(StructField(field_strs[1], IntegerType(), True))
        fields.append(StructField(field_strs[2], StringType(), True))
        fields.append(StructField(field_strs[3], TimestampType(), True))
        fields.append(StructField(field_strs[4], DecimalType(12, 10), True))
        return StructType(fields)

    @staticmethod
    def loan_schema():
        schema_string = "uid,loan_time,loan_amount,plannum"
        field_strs = schema_string.split(",")
        fields = []
        fields.append(StructField(field_strs[0], StringType(), False))
        fields.append(StructField(field_strs[1], TimestampType(), True))
        fields.append(StructField(field_strs[2], DecimalType(12, 10), False))
        fields.append(StructField(field_strs[3], IntegerType(), True))
        return StructType(fields)

    @staticmethod
    def order_schema():
        schema_string = "uid,buy_time,price,qty,cate_id,discount"
        field_strs = schema_string.split(",")
        fields = []
        fields.append(StructField(field_strs[0], StringType(), False))
        fields.append(StructField(field_strs[1], TimestampType(), True))
        fields.append(StructField(field_strs[2], DecimalType(11, 10), False))
        fields.append(StructField(field_strs[3], IntegerType(), True))
        fields.append(StructField(field_strs[4], StringType(), True))
        fields.append(StructField(field_strs[5], DecimalType(11, 10), True))
        return StructType(fields)
