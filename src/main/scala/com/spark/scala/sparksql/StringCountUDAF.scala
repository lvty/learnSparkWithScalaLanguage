package com.spark.scala.sparksql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, types}

//实现自定义的UDAF函数，实现统计字符串的功能
class StringCountUDAF extends UserDefinedAggregateFunction{

  //指定输入数据的类型
  override def inputSchema: StructType = StructType(Array(types.StructField("str",StringType,true)))

  //指的是中间进行聚合时，所处理的数据的类型
  override def bufferSchema: StructType = StructType(Array(types.StructField("count",IntegerType,true)))

  //函数返回值的类型
  override def dataType: DataType = IntegerType

  //为每一个分组的数据执行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  //对于每一个分组，有新的值进来的时候，如何进行分组对应的聚合值的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  //由于分布式计算，所以针对一个分组的数据，可能会在不同的节点上进行局部聚合，就是update，
  // 但是，最后一个分组，在各个节点上的聚合值，要进行merge操作，也就是合并操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  //指的是，一个分组的聚合值，如何通过中间的缓存聚合值，最终返回最后一个最终的聚合值
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }

  override def deterministic: Boolean = {true}


}
