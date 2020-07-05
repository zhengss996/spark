package RDD

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.IntArraySerializer
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.io.Text
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}
import org.apache.spark.{SparkConf, SparkContext}
import util.ORCUtil

// 类注册器反射注册（亲戚的亲戚）
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(Class.forName("java.lang.Class"))
    kryo.register(Class.forName("scala.collection.mutable.WrappedArray$ofInt"))
    kryo.register(classOf[Array[Int]],new IntArraySerializer)
  }
}

class MapJoinKryo
object MapJoinKryo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[*]")
    val classes: Array[Class[_]] = Array[Class[_]](classOf[ORCUtil],classOf[OrcStruct],classOf[Text])
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrationRequired","true")
    conf.set("spark.kryo.registrator","RDD.MyRegistrator")
    conf.registerKryoClasses(classes)

    val sc = new SparkContext(conf)

    val orcUtil = new ORCUtil
    val broadCast: Broadcast[ORCUtil] = sc.broadcast(orcUtil)

    val rdd: RDD[Int] = sc.parallelize(List(1,2))
    val value: RDD[(ORCUtil,Iterable[String])] = rdd.map(f => {
      val orcUtil = broadCast.value
      (orcUtil,"")
    }).groupByKey()

    value.take(200).map(println)
  }
}