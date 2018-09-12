
from pyspark.sql import SparkSession
from data_schema import DataSchema


if __name__ == "__main__":

    spark = SparkSession.builder.appName("loan_distribution").master("local[*]").getOrCreate()

    userSchema = DataSchema.user_schema()
    userDF = spark.read.csv(path="hdfs://master:9000/xhc/jingdong/t_user.csv", header=True, schema=userSchema)
    loanSchema = DataSchema.loan_schema()
    loanDF = spark.read.csv(path="hdfs://master:9000/xhc/jingdong/t_loan.csv", header=True, schema=loanSchema)
    orderSchema = DataSchema.order_schema()
    orderDF = spark.read.csv(path="hdfs://master:9000/xhc/jingdong/t_order.csv", header=True, schema=orderSchema)

    userDF.write.parquet("hdfs://master:9000/xhc/jingdong/t_user.parquet")
    loanDF.write.parquet("hdfs://master:9000/xhc/jingdong/t_loan.parquet")
    loanDF.write.parquet("hdfs://master:9000/xhc/jingdong/t_order.parquet")
