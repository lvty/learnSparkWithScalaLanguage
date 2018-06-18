package com.spark.scala.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object RDD2DataFrameReflection {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)

    val lines: RDD[String] = sc.textFile("F:\\idea\\learnSparkWithScalaLanguage\\src\\data\\students.txt",2)

    //在scala中使用反射方式，进行rdd到DataFrame的转换，需要手动导入一个隐式转换
    import context.implicits._

    val df: DataFrame = lines.map(line => {
      val array: Array[String] = line.split(",")
      Student(array(0).toInt, array(1), array(2).toInt)
    }).toDF//转换为DataFrame

    df.registerTempTable("student")
    val res: DataFrame = context.sql("select * from student where age <= 18")
    /*res.rdd.map(row => {
      Student(row(0).toString.toInt,row(1).toString,row(2).toString.toInt)
    }).foreach(println)*/

    //在scala中，对row的使用比java中的row的使用更加丰富
    //在scala中，可以用row的getas方法，获取指定列名的列
    res.rdd.map(row => Student(row.getAs[Int]("id"),row.getAs[String]("name"),row.getAs[Int]("age")))
              .foreach(println)

    //还可以通过row的getValuesMap方法，获取指定几列的值，返回的是一个map
    res.rdd.map(row => {
      val map: Map[String, Nothing] = row.getValuesMap(Array("id","name","age"))
      Student(map("id").toString.toInt,map("name").toString,map("age").toString.toInt)
    }).foreach(println)

    sc.stop()
  }

  case class Student(id:Int,name:String,age:Int)

}
