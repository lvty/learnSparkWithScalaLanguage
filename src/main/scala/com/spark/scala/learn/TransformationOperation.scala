package com.spark.scala.learn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationOperation {
  def main(args: Array[String]): Unit = {
    //map()
    //filter()
    //flatMap()
    //groupByKey()
    //reduceByKey()
    //sortByKey()
    join()
  }

  def map(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val res: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10),5).map(_ * 2)
    res.foreach(println)
    sc.stop()
  }

  def filter(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val res: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10),5).filter(x => x%2 ==0)
    res.foreach(println)
    sc.stop()
  }

  def flatMap(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    sc.parallelize(Array("hello you","hello me")).flatMap(_.split(" ")).foreach(println)
    sc.stop()
  }

  def groupByKey(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val list = Array(Tuple2("class1",80),Tuple2("class2",60),Tuple2("class1",90),Tuple2("class2",70))
    val rdd: RDD[(String, Int)] = sc.parallelize(list,2)

    rdd.groupByKey().foreach(s => {
      println(s._1);
      s._2.foreach(print)
    }
    )
    sc.stop()
  }

  def reduceByKey(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val list = Array(Tuple2("class1",80),Tuple2("class2",60),Tuple2("class1",90),Tuple2("class2",70))
    val rdd: RDD[(String, Int)] = sc.parallelize(list,2)

    rdd.reduceByKey(_+_).foreach(t =>println(t._1 + "总成绩是: " + t._2))
    sc.stop()
  }

  def sortByKey(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)
    val list = Array(Tuple2("class1",80),Tuple2("class2",60),Tuple2("class1",90),Tuple2("class2",70))
    val rdd: RDD[(String, Int)] = sc.parallelize(list,2)

    rdd.sortBy(t =>t._2,false).foreach(x => println(x._1 + x._2))
    sc.stop()
  }


  def join(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)

    val st = Array(
      Tuple2(1, "x"),
      Tuple2(2, "o"),
      Tuple2(1, "xx")
    )
    val scc = Array(
      Tuple2(1, "100"),
      Tuple2(2, "90"),
      Tuple2(1, "80")
    )

    sc.parallelize(st).join(sc.parallelize(scc)).foreach(sc =>{
      println("st Id :" + sc._1)
      println("st name :" + sc._2._1)
      println("scc score :" + sc._2._2)
    })

    sc.stop()
  }

  def cogroup(): Unit ={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection")
    val sc = new SparkContext(conf)

    val st = Array(
      Tuple2(1, "x"),
      Tuple2(2, "o"),
      Tuple2(1, "xx")
    )
    val scc = Array(
      Tuple2(1, "100"),
      Tuple2(2, "90"),
      Tuple2(1, "80")
    )

    sc.parallelize(st).cogroup(sc.parallelize(scc)).foreach(sc =>{
      println("st Id :" + sc._1)
      println("st name :" + sc._2._1)
      println("scc score :" + sc._2._2)
    })

    sc.stop()
  }
}
