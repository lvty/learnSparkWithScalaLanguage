package com.spark.scala.sparksql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object GenericLoadAndSave {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    val df: DataFrame = sqlc
      .read.load("F:\\idea\\learnSparkWithScalaLanguage\\src\\data\\users.parquet")
    df.select(df.col("name"),df.col("favorite_color"))
      .write.format("json").save("F:\\idea\\learnSparkWithScalaLanguage\\src\\data\\res.txt")

    sc.stop()
  }

}
