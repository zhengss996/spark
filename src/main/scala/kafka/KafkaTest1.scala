//package kafka
//
//import java.io.{ByteArrayInputStream, ObjectInputStream}
//import java.util.Properties
//
//import kafka.consumer._
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.kafka.common.serialization.StringSerializer
//import streaming.StreamingStatusValue
//
//import scala.actors.Actor
//
//class HainiuProducer(val topic: String) extends Actor {
//
//  var producer: KafkaProducer[String, StreamingStatusValue] = _
//
//  def init: HainiuProducer = {
//    val props = new Properties();
//    props.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092");
//    props.put("key.serializer", classOf[StringSerializer].getName);
//    props.put("value.serializer", classOf[HainiuKafkaSerializer].getName);
//    this.producer = new KafkaProducer[String, StreamingStatusValue](props);
//    this
//  }
//
//  override def act(): Unit = {
//    var num = 1
//    while (true) {
//      val messageStr = new String(s"hainiu_${num}")
//      println(s"send:${messageStr}")
//      this.producer.send(new ProducerRecord[String, StreamingStatusValue](this.topic, new StreamingStatusValue(num)))
//      num += 1
//      if (num > 10) num = 0
//      Thread.sleep(3000)
//    }
//  }
//}
//
//object HainiuProducer {
//  def apply(topic: String): HainiuProducer = new HainiuProducer(topic).init
//}
//
//class HainiuConsumer(val topic: String) extends Actor {
//  var consumer: ConsumerConnector = _
//
//  def init: HainiuConsumer = {
//    val pro = new Properties();
//    pro.put("zookeeper.connect", "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181");
//    pro.put("group.id", "group1");
//    pro.put("zookeeper.session.timeout.ms", "60000");
//    this.consumer = Consumer.create(new ConsumerConfig(pro))
//    this
//  }
//
//  override def act(): Unit = {
//    import scala.collection.mutable.HashMap
//    val topicCountMap = new HashMap[String, Int]()
//    topicCountMap += topic -> 1
//    val createMessageStream: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topicCountMap)
//    val kafkaStream: KafkaStream[Array[Byte], Array[Byte]] = createMessageStream.get(topic).get(0)
//    val iterator: ConsumerIterator[Array[Byte], Array[Byte]] = kafkaStream.iterator()
//    while (iterator.hasNext()) {
//      import scala.util.control.Breaks._
//      breakable {
//        val bytes: Array[Byte] = iterator.next().message()
//        if(bytes == null){
//          break()
//        }
//        val bi = new ByteArrayInputStream(bytes)
//        val oi = new ObjectInputStream(bi)
//        val obj: AnyRef = oi.readObject()
//        bi.close()
//        oi.close()
//        val value: StreamingStatusValue = obj.asInstanceOf[StreamingStatusValue]
//
//        println(s"receiver:${value}")
//        Thread.sleep(1)
//      }
//    }
//  }
//}
//
//object HainiuConsumer {
//  def apply(topic: String): HainiuConsumer = new HainiuConsumer(topic).init
//}
//
//object KafkaTest1 {
//  def main(args: Array[String]): Unit = {
//    val topic = "hainiu_test_object"
//    val producer = HainiuProducer(topic)
//    val consumer = HainiuConsumer(topic)
//    producer.start()
//    consumer.start()
//  }
//}