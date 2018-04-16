package com.imooc.spark.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * User: searainbow 
  */
object TransformationOperation {

  def main(args: Array[String]): Unit = {

    //map
    //filter
    //    flatMap
    //    groupByKey
    //    reduceByKey
    //    sortByKey
    readTextFile

    /**
      * map算子案例，将集合中的每一个元素都乘以2
      */
    def map() {
      val conf = new SparkConf()
        .setAppName("map")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val numbers = Array(1, 2, 3, 4, 5)
      val numberRDD = sc.parallelize(numbers, 1)
      val multipleNumbersRDD = numberRDD.map(number => number * 2)
      multipleNumbersRDD.foreach(number => println(number))
    }

    /**
      * filter 算子案例，过滤集合中的偶数
      *
      *
      */
    def filter() {
      val conf = new SparkConf()
        .setAppName("filter")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      val numberRDD = sc.parallelize(numbers, 1)
      val evenNumberRDD = numberRDD.filter(number => number % 2 == 0)
      evenNumberRDD.foreach(number => println(number))


    }

    /**
      * flatMap 案列，将文本拆分成多个单词
      */
    def flatMap() {
      val conf = new SparkConf()
        .setAppName("flatMap")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val lines = Array("hello you", "hello me", "hello world")
      val linesRDD = sc.parallelize(lines, 1)
      val wordsRDD = linesRDD.flatMap { line => line.split(" ") }
      wordsRDD.foreach { word => println(word) }
    }

    /**
      * groupByKey案列，按照班级对成绩进行分组
      */
    def groupByKey() {
      val conf = new SparkConf()
        .setAppName("groupByKey")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val scores = Array(("112011", 97), ("112012", 67), ("112011", 80), ("112012", 76))
      val scoresRDD = sc.parallelize(scores, 1)
      val groupScore = scoresRDD.groupByKey()
      groupScore.foreach(score => {
        println("class:" + score._1);
        score._2.foreach { s => println(s) }
        println("===============")
      })
    }

    /**
      * reduceByKey ，统计每个班的总分
      */
    def reduceByKey() {
      val conf = new SparkConf()
        .setAppName("reduceByKey")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val scores = Array(("112011", 97), ("112012", 67), ("112011", 80), ("112012", 76))
      val scoresRDD = sc.parallelize(scores, 1)
      val totalScores = scoresRDD.reduceByKey(_ + _)
      totalScores.foreach(classScore => println(classScore._1 + ":" + classScore._2))
    }

    /**
      * sortByKey案列：按照学生的份数进行排序
      */
    def sortByKey {
      val conf = new SparkConf()
        .setAppName("sortByKey")
        .setMaster("local")
      val sc = new SparkContext(conf)

      val scores = Array((65, "leo"), (50, "tom"), (100, "marry"), (80, "jack"))
      val scoresRDD = sc.parallelize(scores, 1)
      val sortedScores = scoresRDD.sortByKey(false, 1)
      sortedScores.foreach(score => println(score._2 + " Grade : " + score._1))
    }

    def readTextFile: Unit = {
      val conf = new SparkConf()
        .setAppName("readTextFile")
        .setMaster("local")
      val sc = new SparkContext(conf)

      val fileRDD = sc.textFile("/Users/searainbow/Documents/code/spark/ImoocSparkSQLProject/src/main/resources/test.txt")
      fileRDD.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).foreach(println)
    }

  }
}
