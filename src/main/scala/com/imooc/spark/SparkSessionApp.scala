package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * SparkSession的使用
  */

object SparkSessionApp {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()
    val people = spark.read.json("/Users/searainbow/Documents/code/spark/ImoocSparkSQLProject/resource/people.json")
    people.show()

    spark.stop()
  }
}
