package com.spark.scala.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UDF {
  def main(args: Array[String]): Unit = {
    //计算字符串长度的函数

    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //构造模拟数据
    val names = Array("leo","marry","jack","sbsbsbsb")
    val inRDD: RDD[String] = sc.parallelize(names,2)
    val rowRDD: RDD[Row] = inRDD.map(row =>Row(row))
    val structType = StructType(Array(StructField("name",StringType,true)))

    val df: DataFrame = sqlContext.createDataFrame(rowRDD,structType)

    //注册一张表
    df.registerTempTable("nametable")

    //自定义匿名函数注册
    sqlContext.udf.register("strlen",(str:String) => str.length())

    //使用
    sqlContext.sql("select name,strlen(name) from nametable").collect().foreach(println)

    sc.stop()
  }

}
