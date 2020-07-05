package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkHBaseScan {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkTablePutPartition").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "user_install_status_spark")
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    // 读取hbase的RDD有多少个Partition由HBase表的Region个数决定，这个原理和MR的原理是一样的
    println(hbaseRDD.getNumPartitions)

    hbaseRDD.foreach(t => {
      val f: Array[Byte] = Bytes.toBytes("cf")
      val c: Array[Byte] = Bytes.toBytes("count")
      val rowKey: String = Bytes.toString(t._1.get())
      val count: Int = Bytes.toInt(t._2.getValue(f, c))  // 如果用Int会都是0，如果时Long那么会索引越界异常
      println(s"rowKey:${rowKey},count:${count}")
    })
  }
}
