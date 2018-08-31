package com.ganjunhua.spark.mysqlutils

import java.sql.Connection

import com.ganjunhua.spark.jdproject.sql.LoanThanUserLoanBean

object PutData {
  def addData(LTULB: LoanThanUserLoanBean, conn: Connection): Unit = {
    var sql: StringBuilder = new StringBuilder
    sql.append("insert into loanthanuserloan (uid,loan_amount_sum)")
      .append(" values (?,?)")
    val pstm = conn.prepareStatement(sql.toString())
    pstm.setString(1, LTULB.uid)
    pstm.setDouble(2, LTULB.loan_amount_sum)
    val pstmCnt = pstm.executeUpdate()
    println("PutData.addData = " + pstm)
  }
}
