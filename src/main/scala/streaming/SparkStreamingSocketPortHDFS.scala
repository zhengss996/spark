package streaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

import scala.collection.mutable.ArrayBuffer

object SparkStreamingSocketPortHDFS {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingsocketport").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("nn1.hadoop", 6666)
    val flatMap: DStream[String] = lines.flatMap(_.split(" "))
    val mapToPair: DStream[(String, Int)] = flatMap.map((_, 1))
    val reduceByKey: DStream[(String, Int)] = mapToPair.reduceByKey(_ + _)

    reduceByKey.foreachRDD((r, t) => {
      if (!r.isEmpty()) {
        val value: RDD[String] = r.coalesce(1).mapPartitionsWithIndex((paritionID, f) => {
          val configuration = new Configuration()
          val fs: FileSystem = FileSystem.get(configuration)
          val list: List[(String, Int)] = f.toList
          if (list.length > 0) {

            val format: String = new SimpleDateFormat("yyyyMMddHH").format(new Date)
            val path = new Path(s"hdfs://ns1/user/hadoop/spark/sparkstreamingsocketporthdfs/${paritionID}_${format}")
            val outputStream: FSDataOutputStream = if (fs.exists(path)) {
              fs.append(path)
            } else {
              fs.create(path)
            }

            list.foreach(f => {
              outputStream.write(s"${f._1}\t${f._2}\n".getBytes("UTF-8"))
            })
            outputStream.close()
          }
          new ArrayBuffer[String]().toIterator  // 没有作用，就是避免报错
        })
        value.foreach(f => Unit)  // 触发执行操作action
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}