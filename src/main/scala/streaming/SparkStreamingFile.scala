package streaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingFile {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingfile").setMaster("local[2]")
    conf.set("spark.streaming.fileStream.minRememberDuration", "2592000s")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))
    val localPath = "D:\\tmp\\spark\\sparkstreamingfile\\*\\"
    val hadoopConfig = new Configuration()

    val fileStream: InputDStream[(LongWritable, Text)] = streamingContext.fileStream[LongWritable, Text, TextInputFormat](localPath,
      //设置文件过滤的条件
      (path: Path) => {
        println(path.getName)
        path.getName.endsWith(".txt")
      }, false, hadoopConfig)

    val flatMap: DStream[String] = fileStream.flatMap(_._2.toString.split(" "))
    val mapToPair: DStream[(String, Int)] = flatMap.map((_,1))
    val reduceByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)

    reduceByKey.foreachRDD((r,t) => {
      println(s"count time:${t},${r.collect().toList}")
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}