package streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingSocketFileCogroup {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingfile").setMaster("local[*]")
    //设置读取文件目录的时候文件的生成时间小于一个月，这个参数的单位是秒
    conf.set("spark.streaming.fileStream.minRememberDuration", "2592000s")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("nn1.hadoop",6666)

    val countryCountSocket: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)

    val localPath = "D:\\tmp\\spark\\sparkstreamingfile"
    val hadoopConfig = new Configuration()

    val fileStream: InputDStream[(LongWritable, Text)] = streamingContext.fileStream[LongWritable, Text, TextInputFormat](localPath,
      //设置文件过滤的条件
      (path: Path) => {
        println(path.getName)
        path.getName.endsWith(".conf")
      }, false, hadoopConfig)

    val countryDictFile: DStream[(String, String)] = fileStream.map((a: (LongWritable, Text)) => {
      val strings: Array[String] = a._2.toString.split("\t")
      (strings(0), strings(1))
    })

    countryDictFile.join(countryCountSocket).foreachRDD(r => {
      r.foreach(f => {
        val countryCode: String = f._1
        val countryName: String = f._2._1
        val countryCount: Int = f._2._2
        println(s"countryCode:${countryCode},countryCount:${countryCount},countryName:${countryName}")
      })
    })

//        countryDictFile.cogroup(countryCountSocket).foreachRDD((r,t) => {
//          r.foreach(f => {
//            val countryCode: String = f._1
//            val countryName: Iterable[String] = f._2._1
//            val countryCount: Iterable[Int] = f._2._2
//            println(s"time:${t},countryCode:${countryCode},countryCount:${countryCount},countryName:${countryName}")
//          })
//        })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}