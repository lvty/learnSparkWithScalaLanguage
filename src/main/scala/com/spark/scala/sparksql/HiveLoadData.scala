package com.spark.scala.sparksql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

object HiveLoadData {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("HiveDataSource")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc) //使用的是sparkcont创建的HiveContext

    hiveContext.sql("drop table if exists student_infos") //判断表student_infos表若存在就直接删除
    hiveContext.sql("create table if not exists student_infos (name String,age int)")
    hiveContext.sql("load data local inpath '/root/sparkstudy/scala/resource/student_infos.txt' into table student_infos")

    hiveContext.sql("drop table if exists student_scores")
    hiveContext.sql("create table if not exists student_scores (name String,score int)")
    hiveContext.sql("load data local inpath '/root/sparkstudy/scala/resource/student_scores.txt' into table student_scores")

    val res: DataFrame = hiveContext.sql("select s1.name,s1.age,s2.score from student_infos s1 join student_scores s2  on s1.name = s2.name and s2.score >= 80")

    hiveContext.sql("drop table if exists good_student_infos")
    res.write.saveAsTable("good_student_infos")

    val good_student_infosRows: Array[Row] = hiveContext.table("good_student_infos").collect
    for (r <- good_student_infosRows) {
      println(r)
    }

    sc.stop()
  }

}
