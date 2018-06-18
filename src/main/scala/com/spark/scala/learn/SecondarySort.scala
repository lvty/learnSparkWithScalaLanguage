package com.spark.scala.learn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SecondarySort {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputRDD: RDD[String] = sc.textFile("F:\\idea\\learnSparkWithScalaLanguage\\src\\sort.txt",1)

    val res: RDD[(SecondarySortKey, String)] = inputRDD.map(line => {
      val array: Array[String] = line.split(" ")
      (new SecondarySortKey(array(0).toInt, array(1).toInt), line)
    }).sortByKey()

    res.map(line => line._2).foreach(println)
    sc.stop()
  }

}

class SecondarySortKey(val first:Int, val second:Int) extends Ordered[SecondarySortKey] with Serializable{
   def compare(that: SecondarySortKey): Int = {
    if(this.first - that.first != 0){
      this.first - that.first
    }else{
      this.second - that.second
    }

  }
}
