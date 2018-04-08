package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming整合Flume的第一种方式 采用push （这种方式是flume把数据推给spark，数据容易丢失）
  */

object FlumePushWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Ussage: FlumePushWordCount <hostname> <port>")
      System.exit(1)
    }

    val Array(hostname, port) = args //hostname=0.0.0.0 port=41414

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePushWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    import org.apache.spark.streaming.flume._
    val flumeStream = FlumeUtils.createStream(ssc, hostname, port.toInt)

    flumeStream.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()


    ssc.start()
    ssc.awaitTermination()

  }

}