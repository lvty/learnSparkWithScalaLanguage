package com.spark.scala.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object DailyUv {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //注意！！！！
    //使用Sparksql的内置函数，就必须导入sqlcontext的隐式转换
    import sqlContext.implicits._


    //构造用户访问日志数据，并创建DataFrame,第一列为日期，第二列为用户id
    val userAccessLogs = Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-01,1123",
      "2015-10-01,1126",
      "2015-10-01,1125",
      "2015-10-01,1126",
      "2015-10-01,1123"
    )
    val inRDD: RDD[String] = sc.parallelize(userAccessLogs,5)

    //将rdd转换为DataFrame
    val userAccessLogsRowRDD: RDD[Row] = inRDD.map(row => Row(row.split(",")(0),row.split(",")(1).toInt))
    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("id", IntegerType, true)
    ))
    val userAccessLogsDF: DataFrame = sqlContext.createDataFrame(userAccessLogsRowRDD,structType)

    //使用内置函数，countDistinct
    //UV表示去重后的访问总数
    //首先，对DataFrame调用groupBy方法，对某一列进行分组
    //然后，调用agg()方法，第一个参数，必须必须传入之前在groupBY方法中出现的字段
    //第二个参数，传入countDistinct/sum、first等，spark提供的内置函数
    //内置函数中，传入的参数也是用单引号作为前缀的，
    userAccessLogsDF.groupBy("date")
      .agg('date, countDistinct('id))
      .map(row => Row(row(1),row(2)))
      .foreach(println)


    sc.stop()
  }

}
