package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkTablePut {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkTablePut").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "user_install_status_spark")
    hbaseConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, classOf[TableOutputFormat[NullWritable]].getName)
    hbaseConf.set("mapreduce.job.output.key.class", classOf[NullWritable].getName)
    hbaseConf.set("mapreduce.job.output.value.class", classOf[Put].getName)

    // 拿到一样的连接，一个一个的put，性能不如第二个
    val unit1: RDD[(NullWritable, Put)] = unit.map(t => {
      val put = new Put(Bytes.toBytes("spark_tab_" + t))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(t))
      (NullWritable.get(), put)
    })
    unit1.saveAsNewAPIHadoopDataset(hbaseConf)
  }
}
