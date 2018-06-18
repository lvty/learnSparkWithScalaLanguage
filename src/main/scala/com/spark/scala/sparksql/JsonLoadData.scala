package com.spark.scala.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object JsonLoadData {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val studentsReadJsonDF: DataFrame = sqlContext.read.json("F:\\idea\\learnSparkWithScalaLanguage\\src\\data\\students.json")
    studentsReadJsonDF.registerTempTable("students_infos")
    val goodsStudentsInfo: DataFrame = sqlContext.sql("select name,score from students_infos where score >= 80")
    val studentsName: Array[Any] = goodsStudentsInfo.rdd.map(row => row(0)).collect()

    val jsons = Array(
      "{\"name\":\"leo\",\"class\":\"Class_1\"}",
      "{\"name\":\"marry\",\"class\":\"Class_1\"}",
      "{\"name\":\"jack\",\"class\":\"Class_2\"}"
    )
    val jsonsRDD: RDD[String] = sc.parallelize(jsons,2)
    val studentsClassDF: DataFrame = sqlContext.read.json(jsonsRDD)
    studentsClassDF.registerTempTable("studentsClass")
    var sql = "select name,class from studentsClass where name in ("
    for(i <- 0 until studentsName.length){
      sql += "'" + studentsName(i) + "'"
      if(i < studentsName.length - 1){
        sql += ","
      }
    }
    sql += ")"
    //获取到学生的基本信息
    val studentsAndClassInfoDF: DataFrame = sqlContext.sql(sql)

    //执行join操作
    val resRDD: RDD[(String, (Long, String))] = goodsStudentsInfo.rdd.map(row => (row.getAs[String]("name"),
      row.getAs[Long]("score")))
      .join(studentsAndClassInfoDF.rdd.map(row =>
        (row.getAs[String]("name"),
          row.getAs[String]("class"))))

    val resultRDD: RDD[Row] = resRDD.map(row =>Row(row._1,row._2._1.toLong,row._2._2))

    //将RDD转换为DataFrame
    //构造元数据
    val structType = StructType(Array(
      StructField("name", StringType, true),
      StructField("score", LongType, true),
      StructField("class", StringType, true)
    ))
    val df: DataFrame = sqlContext.createDataFrame(resultRDD,structType)
    df.write.json("F:\\idea\\learnSparkWithScalaLanguage\\src\\data\\RES")

    sc.stop()

  }

}
