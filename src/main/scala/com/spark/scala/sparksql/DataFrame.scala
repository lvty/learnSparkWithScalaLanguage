package com.spark.scala.sparksql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrame {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("")
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)
    val df: DataFrame = context.read.json("F:\\idea\\learnSparkWithScalaLanguage\\src\\data\\students.json")
    df.show()
    df.filter(df.col("age").gt(18)).show()
    df.select(df.col("name"),df.col("age").plus(2).as("年龄")).show()
    sc.stop()
  }

}
