package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * User: searainbow
  * Parquet文件操作
  */
object ParquetApp {

  val resources = "/Users/searainbow/Documents/code/spark/ImoocSparkSQLProject/src/main/resources"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetApp")
        .master("local[2]").getOrCreate()

    val usersDF = spark.read.format("parquet").load(s"file://$resources/users.parquet")

    usersDF.show()

    usersDF.select("name","favorite_color").show()

//    转换格式，可以看做是简单的etl
    usersDF.select("name","favorite_color").write.format("json").save(s"file://$resources/jsonout/")

    spark.stop()
  }

}
