package com.spark.scala.learn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LineCounts {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("F:\\idea\\learnSparkWithScalaLanguage\\src\\wc.txt")
    lines.map(line => (line,1)).reduceByKey(_+_).foreach(x => println(x._1 + "" + x._2))

    sc.stop()
  }

}
