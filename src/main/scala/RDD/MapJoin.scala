package RDD

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.{OrcNewInputFormat, OrcNewOutputFormat, OrcStruct}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.orc.OrcProto.CompressionKind
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import util.{ORCFormat, ORCUtil}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.io.Source

class MapJoin
object MapJoin {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("mapjoin").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val orcPath = "D:\\tmp\\spark\\part-r-00001"
    val hadoopConf = new Configuration()
    val orcFileRDD: RDD[(NullWritable, OrcStruct)] = sc.newAPIHadoopFile(orcPath, classOf[OrcNewInputFormat], classOf[NullWritable], classOf[OrcStruct])

    val dictFilePath = "D:\\tmp\\spark\\country_dict.dat"
    val list: List[String] = Source.fromFile(dictFilePath).getLines().toList

    val map: Map[String, String] = Map[String, String]()

    list.foreach(f => {
      val strings: Array[String] = f.split("\t")
      val code: String = strings(0)
      val countryName: String = strings(1)
      map(code) = countryName
    })

    // 广播变量
    val broadCase: Broadcast[mutable.Map[String, String]] = sc.broadcast(map)
    // 俩个累加器
    val hasCountry: LongAccumulator = sc.longAccumulator
    val noHashCountry: LongAccumulator = sc.longAccumulator  // 匹配不上的累加器


//  快
    val mapRDD: RDD[String] = orcFileRDD.mapPartitions(f => {
      val orcUtil = new ORCUtil
      orcUtil.setORCtype(ORCFormat.INS_STATUS)
      val r1 = new ListBuffer[String]
      val countryMap: mutable.Map[String, String] = broadCase.value

      f.foreach(ff => {
        orcUtil.setRecord(ff._2)
        val countryCode: String = orcUtil.getData("country")
        val countryName: String = countryMap.getOrElse(countryCode, "unknown")
        if (!"".equals(countryName)) {
          hasCountry.add(1L)
          r1 += s"${countryCode}\t${countryName}"
        } else {
          noHashCountry.add(1L)
          r1 += ""
        }
      })
      r1.toIterator
    })
    val orcOutRDD: RDD[(NullWritable, Writable)] = mapRDD.mapPartitions(f => {
      val orcUtil = new ORCUtil
      orcUtil.setORCWriteType("struct<country:string,countryname:string>")
      val r1 = new ListBuffer[(NullWritable, Writable)]
      f.foreach(ff => {
        val strings: Array[String] = ff.split("\t")
        orcUtil.addAttr(strings(0)).addAttr(strings(1))
        r1 += ((NullWritable.get(), orcUtil.serialize()))
      })
      r1.toIterator
    })

    import util.MyPredef.deleteHdfs
    hadoopConf.set("hive.exec.orc.default.compress", CompressionKind.SNAPPY.name())
    hadoopConf.set("orc.create.index", "true")

    val outPath = "C:\\Users\\song\\Desktop\\output\\country_code_name_orc"
    outPath.deletePath

    orcOutRDD.saveAsNewAPIHadoopFile(outPath, classOf[NullWritable], classOf[Writable], classOf[OrcNewOutputFormat], hadoopConf)
    mapRDD.take(20).foreach(println)
    println(hasCountry.value)
    println(noHashCountry.value)


//  慢
//    val unit: RDD[String] = orcFileRDD.map(f => {
//      val orcUtil = new ORCUtil
//      orcUtil.setORCtype(ORCFormat.INS_STATUS)
//      val countryMap: mutable.Map[String, String] = broadCase.value
//      orcUtil.setRecord(f._2)
//
//      val countryCode: String = orcUtil.getData("country")
//      val countryName: String = countryMap.getOrElse(countryCode, "")
//
//      if (!"".equals(countryName)) {
//        hasCountry.add(1L)
//        s"${countryCode}\t${countryName}"
//      } else {
//        noHashCountry.add(1L)
//        ""
//      }
//    })
//    unit.take(20).foreach(println)
//    println(hasCountry.value)
//    println(noHashCountry.value)
  }
}
