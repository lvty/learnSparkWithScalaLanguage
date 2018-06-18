package com.spark.scala.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object StringCountUseUDAF {
  def main(args: Array[String]): Unit = {
    //计算同名的有几个

    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //构造模拟数据
    val names = Array("leo","marry","jack","sbsbsbsb","leo","marry","jack","leo","marry","leo")
    val inRDD: RDD[String] = sc.parallelize(names,5)
    val rowRDD: RDD[Row] = inRDD.map(row =>Row(row))
    val structType = StructType(Array(StructField("name",StringType,true)))

    val df: DataFrame = sqlContext.createDataFrame(rowRDD,structType)

    //注册一张表
    df.registerTempTable("nametable")

    //自定义匿名聚合类
    sqlContext.udf.register("strCount", new StringCountUDAF)

    //使用
    sqlContext.sql("select name,strCount(name) from nametable group by name").collect().foreach(println)

    sc.stop()
  }
}
