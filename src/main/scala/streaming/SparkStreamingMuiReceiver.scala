package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingMuiReceiver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingsocketport").setMaster("local[3]")  // 核心数必须大于2
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))
    val line: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 6666)
    val line1: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 7777)

    import scala.collection.mutable.ListBuffer
    val buffer = new ListBuffer[DStream[String]]
    buffer += line
    buffer += line1
    val value: DStream[String] = streamingContext.union(buffer)

    value.foreachRDD(r => {
      r foreach println
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}