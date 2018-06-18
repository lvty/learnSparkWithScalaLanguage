package com.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowHotWord {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCount")
    val jssc = new StreamingContext(conf,Seconds(5))

    val inDStream: ReceiverInputDStream[String] = jssc.socketTextStream("node1",9999)
    inDStream.map(_.split(" ")(1)).map(x =>(x,1)).reduceByKeyAndWindow((v1:Int,v2:Int) => v1 + v2,Seconds(60),Seconds(10))
      .transform(rdd =>{
        val res: RDD[(String, Int)] = rdd.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
        for(i <- res.take(3)){
          println("SearchWord: " + i._1 + " Count: " + i._2)
        }
        res
      }).print()

    jssc.start()
    jssc.awaitTermination()
  }
}
