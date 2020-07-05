package streaming

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer, Map}

object SparkStreamingKafkaBroadCastUpdate {
  def main(args: Array[String]): Unit = {
    val topic = "hainiu_test"
    val brokers = "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092"
    //这里设置cpu cores为1的时候也能运行，说法KafkaUtils.createDirectStream是不需要receiver占用一个cpu cores的
    //而KafkaUtils.createStream是需要的
    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingkafkabroadcastupdate").setMaster("local[*]")

    //设置blockInterval来调整并发度，也就是分区数
    //这个配置对于StreamingContext创建的流是起作用的，比如socket
    //对于KafkaUtils.createDirectStream（kafka直连模式）创建的流是不起作用的
    //对了KafkaUtils.createStream（kafka的receiver模式）创建的流是起作用的
    //因为直连模式中是根据topic的分区数来决定并发度的，也就是task会直接连接到kafka中topic的partition上
    //conf.set("spark.streaming.blockInterval","1000ms")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))

    val kafkaParams = new HashMap[String, Object]()
    kafkaParams.put("bootstrap.servers", brokers)
    kafkaParams.put("group.id", "group1")
    //这两个配置是做为producer使用的
    //    kafkaParams.put("key.serializer", classOf[StringSerializer].getName)
    //    kafkaParams.put("value.serializer", classOf[StringSerializer].getName)
    kafkaParams.put("key.deserializer", classOf[StringDeserializer].getName)
    kafkaParams.put("value.deserializer", classOf[StringDeserializer].getName)

    //这里创建了3个DStream，并指定了每个DStream负责的partition
    val dStreamList = new ListBuffer[InputDStream[ConsumerRecord[String, String]]]

    for(i <- 0 until 3){
      val partitions = new ListBuffer[TopicPartition]
      for(ii <- (6 * i) until (6 * i) + 6){
        partitions += new TopicPartition(topic,ii)
      }
      val assign: ConsumerStrategy[String,String] = ConsumerStrategies.Assign(partitions,kafkaParams)
      val lines: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,assign)
      dStreamList += lines
    }

    val lines: DStream[ConsumerRecord[String, String]] = streamingContext.union(dStreamList)
    val words: DStream[String] = lines.flatMap(_.value().split(" "))


    var mapBroadCast: Broadcast[Map[String, String]] = streamingContext.sparkContext.broadcast(Map[String,String]())

    //配置更新数据间隔的时间
    val updateInterval = 10000L
    var lastUpdateTime = 0L
    val mathcAccmulator: LongAccumulator = streamingContext.sparkContext.longAccumulator
    val noMathcAccmulator: LongAccumulator = streamingContext.sparkContext.longAccumulator


    words.foreachRDD(r => {
      //第一次启动的时候先进行广播变量的更新操作，以后的更新操作晃根据时间来判断的
      //这里是在driver端进行广播变量的重建
      if(mapBroadCast.value.isEmpty || System.currentTimeMillis() - lastUpdateTime >= updateInterval){
        val map: Map[String, String] = Map[String,String]()
        val dictFilePath = "D:\\tmp\\spark\\updateSparkBroadCast\\updateSparkBroadCast"
        val fs: FileSystem = FileSystem.get(new Configuration())
        val fileStatus: Array[FileStatus] = fs.listStatus(new Path(dictFilePath))
        for(f <- fileStatus){
          val filePath: Path = f.getPath
          val stream: FSDataInputStream = fs.open(filePath)
          val reader = new BufferedReader(new InputStreamReader(stream))

          var line: String = reader.readLine()
          while(line != null){
            val strings: Array[String] = line.split("\t")
            val code: String = strings(0)
            val countryName: String = strings(1)
            map += code -> countryName
            line = reader.readLine()
          }
        }

        //手动取消广播变量的持久化
        mapBroadCast.unpersist(false)
        //重新创建广播变量
        mapBroadCast = streamingContext.sparkContext.broadcast(map)
        //修改最后一次的更新时间
        lastUpdateTime = System.currentTimeMillis()
      }

      println(mapBroadCast.value)

      r.foreachPartition(it => {
        val cast: mutable.Map[String, String] = mapBroadCast.value
        it.foreach(f => {
          println(s" ${f} ")

          //scala的countinue的写法
          import scala.util.control.Breaks._
          breakable {
            if(f == null){
              break()
            }
            if(cast.contains(f)){
              mathcAccmulator.add(1L)
            }else{
              noMathcAccmulator.add(1L)
            }
          }
        })
      })

      val matchA: Long = mathcAccmulator.count
      val noMatch: Long = noMathcAccmulator.count

      println(s"match:${matchA},noMatch:${noMatch}")

      //累加器清0
      mathcAccmulator.reset()
      noMathcAccmulator.reset()
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
