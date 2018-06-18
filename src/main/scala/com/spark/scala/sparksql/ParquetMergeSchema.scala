package com.spark.scala.sparksql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ParquetMergeSchema {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    val studentsWithNameAndAge = Array(("leo",23),("jack",25))
    import sqlc.implicits._
    val studentsWithNameAndAgeDF: DataFrame = sc.parallelize(studentsWithNameAndAge,2).toDF("name","age")
    studentsWithNameAndAgeDF.write.mode("append").save("F:\\idea\\learnSparkWithScalaLanguage\\src\\data\\stu")

    // 创建第二个DataFrame，作为学生的成绩信息，并写入一个parquet文件中
    val studentsWithNameAndGrade = Array(("marry","A"),("tom","B"))
    val studentsWithNameAndGradeDF: DataFrame = sc.parallelize(studentsWithNameAndGrade,2).toDF("name","grade")
    studentsWithNameAndGradeDF.write.mode("append").save("F:\\idea\\learnSparkWithScalaLanguage\\src\\data\\stu")

    val resDF: DataFrame = sqlc.read.option("mergeSchema","true").load("F:\\idea\\learnSparkWithScalaLanguage\\src\\data\\stu")
    resDF.printSchema()
    resDF.show()
    sc.stop()
  }

}
