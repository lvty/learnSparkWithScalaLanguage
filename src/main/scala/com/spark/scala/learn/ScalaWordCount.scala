package com.spark.scala.learn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("F:\\idea\\learnSparkWithScalaLanguage\\src\\wc.txt")

    val line: RDD[String] = lines.flatMap(_.split(" "))
    val res1: RDD[(String, Int)] = line.map((_,1)).reduceByKey(_+_)
    val res: RDD[(String, Int)] = res1.map(wc =>(wc._2,wc._1)).sortByKey(false).map(wc => (wc._2,wc._1))
    res.foreach(x => println(x._1 + "\t" + x._2))

    sc.stop()
  }

}