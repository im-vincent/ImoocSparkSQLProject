package com.imooc.spark.basic

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * UDAF聚合
  * 多行输入，返回一个输出
  */
class UDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("nums", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("cnt", LongType) ::
      StructField("avg", DoubleType) :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0l
    buffer(1) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) + input.getAs[Double](0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  override def evaluate(buffer: Row): Any = {
    val t = buffer.getDouble(1) / buffer.getLong(0)
    f"$t%1.5f".toDouble
  }
}
