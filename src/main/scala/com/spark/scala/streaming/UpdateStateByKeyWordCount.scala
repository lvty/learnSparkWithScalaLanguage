package com.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCount")
    val jssc = new StreamingContext(conf,Seconds(5))

    jssc.checkpoint("hdfs://ll/wordcount_checkpoint")

    val lines = jssc.socketTextStream("node1", 9999)
    val words = lines.flatMap { _.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val res = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValue = state.getOrElse(0)
      for(value <- values) {
        newValue += value
      }
      Option(newValue)
    })

    res.print()


    jssc.start()
    jssc.awaitTermination()
  }

}
