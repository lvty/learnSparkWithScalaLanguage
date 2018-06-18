package com.spark.scala.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object RDD2DataFrameProgrammatically {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local[*]"))
    val context = new SQLContext(sc)
    val lines: RDD[String] = sc.textFile("F:\\idea\\learnSparkWithScalaLanguage\\src\\data\\students.txt",2)
    val linesRDD: RDD[Row] = lines.map(line => {
      val a: Array[String] = line.split(",")
      Row(a(0).toInt, a(1), a(2).toInt)
    })

    val structType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    val df: DataFrame = context.createDataFrame(linesRDD,structType)
    df.registerTempTable("student")
    val res: DataFrame = context.sql("select * from student where age <= 18")
    res.rdd.map(line =>{
      Row(line.getAs("id"),line.getAs("name"),line.getAs("age"))
    }).foreach(println)
    sc.stop()
  }
}
