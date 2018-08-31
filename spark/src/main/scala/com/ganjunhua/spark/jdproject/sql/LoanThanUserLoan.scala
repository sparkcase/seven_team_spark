package com.ganjunhua.spark.jdproject.sql

import java.util.Properties

import com.ganjunhua.spark.mysqlutils.DbUtils
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object LoanThanUserLoan {

  /**
    * 功能：
    * 1、借款金额超过2000且购买商品总价值超过借款总金额的用户ID
    * 2、从不买打折产品且不借款的用户ID
    */
  /**
    * 订单表
    *
    * @param uid      ：用户ID
    * @param buy_time ：购买时间
    * @param price    :价格
    * @param qty      ：数量
    * @param cate_id  ：品类ID
    * @param discount ：优惠金额
    */
  case class T_order(uid: String, buy_time: String, price: Double, qty: Int, cate_id: String, discount: Double)

  /**
    * 定义贷款表
    *
    * @param uid         ：用户ID
    * @param loan_time   ：借款时间
    * @param loan_amount ：借款金额
    * @param plannum     ：分期期数
    */
  case class T_loan(uid: String, loan_time: String, loan_amount: Double, plannum: String)

  /**
    * 定义用户表
    *
    * @param uid         ：用户ID
    * @param age         ：年龄段
    * @param sex         ：性别
    * @param active_date ：用户激活日期
    * @param limit_amt   ：初始额度
    */
  case class T_user(uid: String, age: String, sex: String, active_date: String, limit_amt: Double)

  def dontBuySale(spark: SparkSession, dataPath: String): Unit = {
    /**
      * 通过反射方式，为RDD注入schema,将其转换为DataFrame
      */
    //T_order(uid: String, buy_time: String, price: Double, qty: Int, cate_id: String, discount: Double)
    import spark.implicits._
    /**
      * 订单表
      */
    // 创建表头
    val torderSchemaString = "uid buy_time price qty cate_id discount"
    val torderSchema = StructType(torderSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, true)))
    //加载数据
    val torderData = spark.sparkContext.textFile(dataPath + "/t_order.csv")
    //去除数据表头
    val torderFirst = torderData.first()
    val torderRDD_1 = torderData.filter(x => x != torderFirst)
    //绑定表头
    val torderRDD = torderRDD_1.map(x => x.split(","))
      .map(x => Row(
        x(0).trim,
        x(1).trim,
        x(2).trim,
        x(3).trim,
        x(4).trim,
        x(5).trim))
    // 创建 DataFrame
    val torderDF = spark.createDataFrame(torderRDD, torderSchema)


    /**
      * 定义借款表
      */
    //T_loan(uid: String, loan_time: String, loan_amount: Double, plannum: String)
    // 创建表头
    val tloanSchemaString = "uid loan_time loan_amount plannum"
    val tloanSchema = StructType(tloanSchemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, true)))
    //加载数据
    val tloanData = spark.sparkContext.textFile(dataPath + "/t_loan.csv")
    //去除数据表头
    val tloanFirst = tloanData.first()
    val tloanRDD_1 = tloanData.filter(x => x != tloanFirst)
    //绑定表头
    val tloanRDD = tloanRDD_1.map(x => x.split(","))
      .map(x => Row(x(0).trim, x(1).trim, x(2).trim, x(3).trim))
    // 创建 DataFrame
    val tloanDF = spark.createDataFrame(tloanRDD, tloanSchema)

    //计算用户打折金额
    val torderDFSum = torderDF.groupBy("uid").agg(sum('discount) as "sum_discount")
      .select("uid", "sum_discount")

    //计算用户借款金额
    val tloanRDDSum = tloanDF.groupBy("uid").agg(sum('loan_amount) as "sum_loan_amount")
      .select("uid", "sum_loan_amount")

    //从不买打折产品且不借款的用户ID
    val torderTloan = torderDFSum.join(tloanRDDSum, torderDFSum("uid") === tloanRDDSum("uid")
      and torderDFSum("sum_discount") === 0
      and tloanRDDSum("sum_loan_amount") === 0
      , "inner").select(
      torderDFSum("uid"),
      torderDFSum("sum_discount") as "price_sum",
      tloanRDDSum("sum_loan_amount") as "loan_amount_sum")
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "admin123")
    val tableName = "loanthanuserloan"
    torderTloan.show(10)
    println("torderTloan.printSchema()")
    torderTloan.write.mode(SaveMode.Append).jdbc(DbUtils.url, tableName, prop)
  }

  def loanThanUid(spark: SparkSession, dataPath: String): Unit = {
    /**
      * ：通过显式为RDD注入schema,将其变换为DataFrame
      */
    import spark.implicits._
    // 获取数据
    val userData = spark.sparkContext.textFile(dataPath + "/t_user.csv")
    val userFirst = userData.first()
    val userRDD_1 = userData.filter(x => x != userFirst)
    val userRDD = userRDD_1.map(x => x.split(","))
      .map(x => T_user(x(0), x(1), x(2), x(3), x(4).toDouble))
    val userDF = userRDD.toDF()

    val loanData = spark.sparkContext.textFile(dataPath + "/t_loan.csv")
    val loanFirst = loanData.first()
    val loanRDD_1 = loanData.filter(x => x != loanFirst)
    val loanRDD = loanRDD_1.map(x => x.split(","))
      .map(x => T_loan(x(0), x(1), x(2).toDouble, x(3)))
    val loanDF = loanRDD.toDF()

    val orderData = spark.sparkContext.textFile(dataPath + "/t_order.csv")
    val orderFirst = orderData.first()
    val orderRDD_1 = orderData.filter(x => x != orderFirst)
    val orderRDD = orderRDD_1.map(x => x.split(","))
      .map(x => T_order(x(0),
        x(1),
        if (x(2) == Nil) 0 else x(2).toDouble,
        if (x(3) == Nil) 0 else x(3).toInt, x(4),
        if (x(5) == null) 0 else x(5).toDouble))
    val orderDF = orderRDD.toDF()

    //借款金额超过2000
    val loanDFSum_1 = loanDF.groupBy("uid").agg(sum('loan_amount) as "loan_amount_sum")
      .select("uid", "loan_amount_sum")
    val loanDFSum = loanDFSum_1.filter("loan_amount_sum > 20")
    //购买商品总价值
    val orderDFSum = orderDF.groupBy("uid").agg(sum('price * 'qty) as "price_sum")
      .select("uid", "price_sum")

    //借款金额超过2000且购买商品总价值超过借款总金额的用户ID
    val loanOrder = orderDFSum.join(loanDFSum, orderDFSum("uid") === loanDFSum("uid")
      and orderDFSum("price_sum") > loanDFSum("loan_amount_sum"), "inner")
      .select(orderDFSum("uid"), orderDFSum("price_sum"), loanDFSum("loan_amount_sum"))
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "admin123")
    val tableName = "loanthanuserloan"
    println(DbUtils.url)
    loanOrder.write.mode(SaveMode.Append).jdbc(DbUtils.url, tableName, prop)
    loanOrder.show(10)
    DbUtils.justPrint()

  }

  def main(args: Array[String]): Unit = {
    // 数据路径
    var dataPath: String = "data/jd"
    //运行模式
    var masterType: String = "local[*]"
    //运行appName
    var appName: String = this.getClass.getSimpleName
    if (args.length > 2) {
      appName = args(0)
      masterType = args(1)
      dataPath = args(2)
    }
    //定义唯一入口
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master(masterType)
      .getOrCreate()
    //1、借款金额超过2000且购买商品总价值超过借款总金额的用户ID
    loanThanUid(spark, dataPath)
    //2、从不买打折产品且不借款的用户ID
    dontBuySale(spark, dataPath)
    spark.stop()
  }
}
