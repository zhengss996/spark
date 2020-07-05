package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketPort {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingsocketport").setMaster("local[2]")
    val streamingContext = new StreamingContext(conf,Durations.seconds(5))

    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("nn1.hadoop",6666)

    //    lines.foreachRDD((r,t) => {
    //      val flatMap: RDD[String] = r.flatMap(_.split(" "))
    //      val mapToPair: RDD[(String, Int)] = flatMap.map((_,1))
    //      val reduceByKey: RDD[(String, Int)] = mapToPair.reduceByKey(_ + _)
    //      println(s"count time:${t},${reduceByKey.collect().toList}")
    //    })

    val flatMap: DStream[String] = lines.flatMap(_.split(" "))

    val mapToPair: DStream[(String, Int)] = flatMap.map((_,1))

    //    val reduceByKey: DStream[(String, Int)] = flatMap.transform(r => {
    //      val mapToPair: RDD[(String, Int)] = r.map((_, 1))
    //      val reduceByKey: RDD[(String, Int)] = mapToPair.reduceByKey(_ + _)
    //      reduceByKey
    //    })

    val reduceByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)

    reduceByKey.foreachRDD((r,t) => {
      println(s"count time:${t},${r.collect().toList}")
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}