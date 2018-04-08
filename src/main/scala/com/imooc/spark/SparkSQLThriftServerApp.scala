package com.imooc.spark

import java.sql.DriverManager


/**
  * 通过jdbc的方式访问
  */
object SparkSQLThriftServerApp {

  def main(args: Array[String]) {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://192.168.56.101:10000", "hadoop", "")
    val pstmt = conn.prepareStatement("select * from emp")
    val rs = pstmt.executeQuery()
    while (rs.next()) {
      println("empno:" + rs.getInt("empno"))
    }

    rs.close()
    pstmt.close()
    conn.close()


  }


}