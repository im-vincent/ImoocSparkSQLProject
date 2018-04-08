package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * User: searainbow 
  */
object JDBCApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JDBCApp").master("local[2]").getOrCreate()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.56.101:3306")
      .option("dbtable", "hive.TBLS")
      .option("user", "wangxi")
      .option("password", "wangxi")
      .load()

    jdbcDF.show()
  }

}
