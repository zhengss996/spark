package streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable.HashMap

object SparkStreamingKafka {
  def main(args: Array[String]): Unit = {
    val topic = "hainiu_test"
    val brokers = "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092"
    //这里设置cpu conres为1的时候也未能运行，说明kafkaUtils.createDirectStream是不需要receiver占用一个cpu cores
    //而KafkaUtils.createStream是需要的
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingKafka").setMaster("local[1]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(5))

    val topicSet: Set[String] = topic.split(",").toSet

    val kafkaParams = new HashMap[String, Object]()
    kafkaParams.put("bootstrap.servers", brokers)
    kafkaParams.put("group.id", "group1")
    kafkaParams.put("key.deserializer", classOf[StringDeserializer].getName)
    kafkaParams.put("value.deserializer", classOf[StringDeserializer].getName)

    val offset = new HashMap[TopicPartition, Long]()
    //这里虽然指定了读取的partitionid，但是并没有生效，因为consumer使用的是Subscribe（订阅）模式
    //当使用分配模式的时候是可以根据指定的partitionid，去读取特定的partition的
    offset += new TopicPartition(topic, 0) -> 1

    val value: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(topicSet, kafkaParams, offset)
    // KafkaUtils相当于streamingContext，因为kafka使用的是自己的      createDirectStream:创建一个直接的流模式    如果集群和kafka在一起，用这个
    val lines: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, value)

    val reduceByKey: DStream[(String, Int)] = lines.flatMap(_.value().split(" ")).map((_,1)).reduceByKey(_ + _)

    reduceByKey.foreachRDD((r,t) => {
      println(s"count time:${t},${r.collect().toList}")
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
