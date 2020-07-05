package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object SparkStreamingSocketPortUpdateState {
  def main(args: Array[String]): Unit = {

    val checkPointPath = "C:\\Users\\song\\Desktop\\output\\sparkstreamingsocketport"

    val strc:StreamingContext = StreamingContext.getOrCreate(checkPointPath, func)

    strc.start()
    strc.awaitTermination()
  }

  private def func: () => StreamingContext = {
    () => {
      val conf: SparkConf = new SparkConf().setAppName("sparkstreamingsocketport").setMaster("local[2]")
      val streamingContext = new StreamingContext(conf, Durations.seconds(5))
      streamingContext.checkpoint("C:\\Users\\song\\Desktop\\output\\sparkstreamingsocketport")

      val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 6666)
      val flatMap: DStream[String] = lines.flatMap(_.split(" "))
      val mapToPair: DStream[(String, Int)] = flatMap.map((_, 1))
      val reduceByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)

      val updateStateByKey: DStream[(String, Int)] = reduceByKey.updateStateByKey((a: Seq[Int], b: Option[Int]) => {
        var total = 0
        for (i <- a) {
          total += i
        }
        val last: Int = if (b.isDefined) b.get else 0
        val now = total + last
        Some(now)
      })

      updateStateByKey.foreachRDD((r, t) => {
        println(s"count time:${t},${r.collect().toList}")
      })
      streamingContext
    }
  }
}