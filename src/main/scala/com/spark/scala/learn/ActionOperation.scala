package com.spark.scala.learn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {
  def main(args: Array[String]): Unit = {
    //reduce()
    //collect()
    //take()
    //saveAsTextFile()
    countByKey();
  }

  def reduce(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val list = Array(1,2,3,4,5)

    val input: RDD[Int] = sc.parallelize(list)
    println(input.reduce(_+_))

    sc.stop()
  }

  def collect(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val list = Array(1,2,3,4,5)

    val res: Array[Int] = sc.parallelize(list).map(_ * 2).collect()

    for(i <- res){
      println(i)
    }

    sc.stop()
  }

  def take(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val list = Array(1,2,3,4,5)

    val res: Array[Int] = sc.parallelize(list).take(3)

    for(i <- res){
      println(i)
    }

    sc.stop()
  }

  def saveAsTextFile(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val list = Array(1,2,3,4,5)

    sc.parallelize(list).map(_ * 2).saveAsTextFile("F:\\idea\\learnSparkWithScalaLanguage\\src\\saveFiles.txt")

    sc.stop()
  }

  def countByKey(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val list = Array(Tuple2("class1",80),Tuple2("class2",60),Tuple2("class1",90),Tuple2("class2",70))
    val rdd: RDD[(String, Int)] = sc.parallelize(list,2)

    val res: collection.Map[String, Long] = rdd.countByKey()
    for((k,v) <- res){
      println(k +" \t" +v)
    }
    sc.stop()
  }

}
