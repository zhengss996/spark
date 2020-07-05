package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkHBaseScanStartStop {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkTablePutPartition").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val hbaseConf: Configuration = HBaseConfiguration.create()

    // 获取表的范围
    val scan: Scan = new Scan()
    scan.addFamily(Bytes.toBytes("cf"))
    scan.setCaching(1000)
    scan.setCacheBlocks(false)  // 不设置缓存
    scan.setStartRow(Bytes.toBytes("spark_batch_"))
    scan.setStopRow(Bytes.toBytes("spark_batch_z"))

    hbaseConf.set(TableInputFormat.INPUT_TABLE, "user_install_status_spark")
    hbaseConf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan))

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    // 读取hbase的RDD有多少个Partition由HBase表的Region个数决定，这个原理和MR的原理是一样的
    println(hbaseRDD.getNumPartitions)

    hbaseRDD.foreach(t => {
      val f: Array[Byte] = Bytes.toBytes("cf")
      val c: Array[Byte] = Bytes.toBytes("count")
      val rowKey: String = Bytes.toString(t._1.get())
      val count: Int = Bytes.toInt(t._2.getValue(f, c))
      println(s"rowKey:${rowKey},count:${count}")
    })
  }
}