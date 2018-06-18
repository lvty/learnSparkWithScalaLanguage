package com.spark.scala.learn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupedTop3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)

    val inputRDD: RDD[String] = sc.textFile("F:\\idea\\learnSparkWithScalaLanguage\\src\\score.txt")
    val grouped: RDD[(String, Iterable[Int])] = inputRDD.map(line => {
      val array: Array[String] = line.split(" ")
      (array(0), array(1).toInt)
    }).groupByKey()
    val sortedRDD: RDD[(String, List[Int])] = grouped.map(line => {
      val className: String = line._1
      val values: Iterable[Int] = line._2
      val list: List[Int] = values.toList.sorted
      //返回top3
      val sorted: List[Int] = list
      (className, List(sorted(list.size - 1), sorted(list.size - 2), sorted(list.size - 3)))
    })
    sortedRDD.foreach(line =>{
      val className: String = line._1
      println("className: " + className)
      val v: List[Int] = line._2
      for(i <- v){
        println(i)
      }
      println("====================")
    })


    sc.stop()
  }

}
