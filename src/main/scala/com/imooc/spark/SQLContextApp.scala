package com.imooc.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SQLContext使用
  */

object SQLContextApp {
  def main(args: Array[String]) {

    val path = args(0)

    //    1.创建相应的Context
    val sparkConf = new SparkConf()

    //    在测试或者生产中，AppName和Master我们是通过脚本进行执行
    //    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //    2. 相关的处理: json
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    //    3. 关闭资源
    sc.stop()
  }

}
