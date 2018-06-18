package com.spark.scala.learn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Top3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("F:\\idea\\learnSparkWithScalaLanguage\\src\\top.txt")


    lines.map(line =>(line.toInt,line)).sortByKey(false).map(line =>line._2).take(3).foreach(println)

    sc.stop()
  }

}
