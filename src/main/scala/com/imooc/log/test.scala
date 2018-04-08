package com.imooc.log
import java.util.{Date, Locale}

object test {
  def main(args: Array[String]): Unit = {
    val time = "[10/Nov/2016:00:01:02 +0800]"
    println((time.substring(1,time.lastIndexOf("]"))))
  }
}
