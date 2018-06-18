package com.spark.scala.learn

import org.apache.spark.{Accumulator, SparkConf, SparkContext}

object AccummuatorVariable{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val list = Array(1,2,3,4,5)
    val accum: Accumulator[Int] = sc.accumulator(0)

    sc.parallelize(list,2).foreach(num => accum += num)
    println(accum.value)
    sc.stop()
  }
}