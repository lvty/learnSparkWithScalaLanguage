package com.spark.scala.learn

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeCollection {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val res: Int = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10),5).reduce(_+_)
    println(res)
    sc.stop()
  }

}
