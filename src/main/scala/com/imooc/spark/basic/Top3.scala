package com.imooc.spark.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 取最大的前三个数
  */
object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3").setMaster("local")
    val sc = new SparkContext(conf)

    val resources = "/Users/searainbow/Documents/code/spark/ImoocSparkSQLProject/src/main/resources"

    val lines = sc.textFile(s"file://$resources/top.txt")
    val pairs = lines.map(line => (line.toInt, line))
    val sortPairs = pairs.sortByKey(false)
    val sortNumbers = sortPairs.map(t => t._2)
    val top3 = sortNumbers.take(3)
    top3.foreach(println)
    sc.stop()

  }
}
