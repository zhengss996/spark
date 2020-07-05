package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketPortUpdateStateWindows {
  def main(args: Array[String]): Unit = {

    val checkpointPath = "C:\\Users\\song\\Desktop\\output\\sparkstreamingsocketportupdatestatewindows"
    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingsocketport").setMaster("local[2]")
    val streamingContext = new StreamingContext(conf,Durations.seconds(5))
    streamingContext.checkpoint(checkpointPath)
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost",6666)

    val flatMap: DStream[String] = lines.flatMap(_.split(" "))
    val mapToPair: DStream[(String, Int)] = flatMap.map((_,1))

    //这个是使用了磁盘存储windows的数据，所以必须设置checkpoint的地址，并直接返回统计好的值
    //    val value: DStream[(String, Long)] = flatMap.countByValueAndWindow(Durations.seconds(20),Durations.seconds(10))

    //这个使用了内存保存了windows的数据，不用设置checkpoint的地址，使用了DStream的countryByValue(),代替了recucdeByKey，返回统计好的值
    //sparkStreaming使用内存的方法有，比如DStream的cache、presist、windows还有streamingContext.remeber
    //如果还有就是咱们使用了foreacheRDD之后把里面的RDD进行cache或presist
    //    val value: DStream[(String, Long)] = flatMap.window(Durations.seconds(20),Durations.seconds(10)).countByValue()

    //如果窗口函数中不写划动间隔那窗口的运算时间就和批次间隔一致
    val reduceByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _).window(Durations.seconds(20),Durations.seconds(10))

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

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}