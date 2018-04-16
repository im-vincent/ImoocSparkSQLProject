package com.imooc.spark.basic

import org.apache.spark.sql.SparkSession


/**
  * UDF函数
  */
object UDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UDF")
      .master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    //    构造模拟数据
    val name = Array("tom", "leo", "wangxi")
    val nameRDD = sc.parallelize(name, 2)

    //    转换成DF
    import spark.implicits._
    val studentDF = nameRDD.map(_.split(",")).map(line => Student(line(0).toString)).toDF()
    //    注册成表
    studentDF.createOrReplaceTempView("student")
    //    注册udf
    spark.udf.register("strlen", (str: String) => str.length())

    spark.sql("select name,strlen(name) from student").collect().foreach(println)

    spark.stop()
  }

  case class Student(name: String)

}
