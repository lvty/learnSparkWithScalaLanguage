package com.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformBlackList {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCount")
    val jssc = new StreamingContext(conf,Seconds(5))

    val blackList = Array(("bob",true))
    val blackListRDD: RDD[(String, Boolean)] = jssc.sparkContext.parallelize(blackList,5)

    val clickLogsDStream: ReceiverInputDStream[String] = jssc.socketTextStream("node1",9999)
    clickLogsDStream.map(line => (line.split(" ")(1),line))
            .transform(upstream =>{
              val joinedRDD: RDD[(String, (String, Option[Boolean]))] = upstream.leftOuterJoin(blackListRDD)
              val filteredRDD: RDD[(String, (String, Option[Boolean]))] = joinedRDD.filter(t => {
                if (t._2._2.getOrElse(false)) {
                  false
                }else{
                  true
                }
              })
              filteredRDD.map(t => t._2._1)
            }).print()

    jssc.start()
    jssc.awaitTermination()
  }

}
