package kafka

import java.util.Properties

import kafka.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.actors.Actor

class HainiuProducer(val topic: String) extends Actor {

  var producer: KafkaProducer[String, String] = _

  def init: HainiuProducer = {
    val props = new Properties();
    // 生产者指定的是broker
    props.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092");
    // key.serializer 和 value.serializer 表示的是发送的java字符串变成二进制
    props.put("key.serializer", classOf[StringSerializer].getName);
    props.put("value.serializer", classOf[StringSerializer].getName);
    this.producer = new KafkaProducer[String, String](props);
    this
  }

  override def act(): Unit = {
    var num = 1
    while (true) {
      val messageStr = new String(s"hainiu_${num}")
      println(s"send:${messageStr}")
      //                                 上面发送的是String,String
      this.producer.send(new ProducerRecord[String, String](this.topic, messageStr))
      num += 1
      if (num > 10) num = 0
      Thread.sleep(3000)
    }
  }
}
object HainiuProducer {
  def apply(topic: String): HainiuProducer = new HainiuProducer(topic).init  // 用于初始化工作
}


class HainiuConsumer(val topic: String) extends Actor {
  var consumer: ConsumerConnector = _

  def init: HainiuConsumer = {
    val pro = new Properties();
    // 消费者指定的是zookeeper
    pro.put("zookeeper.connect", "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181");
    pro.put("group.id", "group1");  // 消费者要指定自己的组，否则系统默认分配一个
    pro.put("zookeeper.session.timeout.ms", "60000");  // 超时时间
    this.consumer = Consumer.create(new ConsumerConfig(pro))
    this
  }

  override def act(): Unit = {
    import scala.collection.mutable.HashMap
    val topicCountMap = new HashMap[String, Int]()
    topicCountMap += topic -> 1  // 表示topic的值从头开始读取
    // 获取读出来的数据，map类型
    val createMessageStream: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topicCountMap)
    // 从topic中拿出第0个
    val kafkaStream: KafkaStream[Array[Byte], Array[Byte]] = createMessageStream.get(topic).get(0)
    val iterator: ConsumerIterator[Array[Byte], Array[Byte]] = kafkaStream.iterator()
    while (iterator.hasNext()) {
      val bytes: Array[Byte] = iterator.next().message()
      println(s"receiver:${new String(bytes)}")
      Thread.sleep(1)
    }
  }
}
object HainiuConsumer {
  def apply(topic: String): HainiuConsumer = new HainiuConsumer(topic).init
}


object KafkaTest {
  def main(args: Array[String]): Unit = {
    val topic = "hainiu_test"
    val producer = HainiuProducer(topic)
    val consumer = HainiuConsumer(topic)
    producer.start()
    consumer.start()
  }
}
