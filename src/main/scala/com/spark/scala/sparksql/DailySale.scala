package com.spark.scala.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object DailySale {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //模拟数据,单独统计网站登录用户的销售额的，有些时候，会出现日志的上报错误和异常，比如日志里丢了用户的信息，
    // 那么这种情况我们就不再统计了
    val dailySaleLogs = Array(
      "2015-10-01,11.22,1133",
      "2015-10-02,112.2,1122",
      "2015-10-03,11.23,1133",
      "2015-10-01,11.24,",
      "2015-10-02,112.4,",
      "2015-10-02,1.123,1123",
      "2015-10-01,112.6,",
      "2015-10-01,1.125,1166"
    )

    val inRDD: RDD[String] = sc.parallelize(dailySaleLogs,5)
    //进行有效日志的过滤’
    val filteredRDD: RDD[String] = inRDD.filter(log => {
      log.split(",").length == 3
    })

    val rdd: RDD[Row] = filteredRDD.map(row => Row(row.split(",")(0),row.split(",")(1).toDouble))

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("sale", DoubleType, true)
    ))
    val df: DataFrame = sqlContext.createDataFrame(rdd,structType)
    df.groupBy("date").agg('date,sum('sale)).map(row => Row(row.get(1),row.get(2))).foreach(println)


    sc.stop()
  }

}
