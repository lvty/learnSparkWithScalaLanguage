package com.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("")
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")
    val ssc = new StreamingContext(conf,Seconds(1))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val wc: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map(x => (x,1)).reduceByKey(_+_)

    Thread.sleep(5)
    wc.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
