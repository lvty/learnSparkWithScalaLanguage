package com.spark.scala.learn

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object BroadCastVariable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val list = Array(1,2,3,4,5)
    val bc: Broadcast[Int] = sc.broadcast(3)
    sc.parallelize(list).map(x => x * bc.value).foreach(println)
    sc.stop()

  }

}
