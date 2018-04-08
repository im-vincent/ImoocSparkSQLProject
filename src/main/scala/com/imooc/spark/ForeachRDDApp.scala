package com.imooc.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成词频统计，并将结果写入到MySQL数据库中
  *
  * create database imooc_spark;
  *
  * create table wordcount (
  * word varchar(50) default null,
  * wordcount int(10) default null
  * )
  *
  */
object ForeachRDDApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val connection = createConnection()
        partition.foreach(record => {
          val sql = "insert into wordcount values (?,?)"
          val ps = connection.prepareStatement(sql)
          ps.setString(1, record._1)
          ps.setInt(2, record._2)
          ps.executeUpdate()
          ps.close()
          connection.close()
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取MySQL的连接
    */
    def createConnection() = {
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://192.168.56.101:3306/imooc_spark", "wangxi", "wangxi")
    }

}
