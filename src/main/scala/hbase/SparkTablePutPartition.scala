package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object SparkTablePutPartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkTablePut").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // val unit: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val orcPath = "D:\\tmp\\spark\\part-r-00001"

    // HiveContext 可以操作 hive 的所有的东西
    val hivec = new HiveContext(sc)
    // 读取 orc 文件
    val df: DataFrame = hivec.read.orc(orcPath)
    // 将数据集注册成表名
    df.createOrReplaceTempView("user_install_status_spark")
    val sql: DataFrame = hivec.sql("select pkgname, count(1) from  user_install_status_spark group by pkgname")
    val rdd: RDD[Row] = sql.rdd

    // 这里打印的是200, 因为spark-sql的shuffle的时候默认的partition是200, 所以这个DF转出来的RDD就是200个partition
    println(rdd.getNumPartitions)
    // 更改分区
    val repartition: RDD[Row] = rdd.repartition(300)
    // 累加器
    val acc: LongAccumulator = sc.longAccumulator

    val hbaseRDD: RDD[(ImmutableBytesWritable, Put)] = repartition.mapPartitions(f => {
      val list: List[Row] = f.toList
      //      println(list)
      // 因为上面给它变成了300个partition，所以这里的累加器就会累加300次
      acc.add(1L)
      import scala.collection.mutable.ListBuffer
      val list1 = new ListBuffer[(ImmutableBytesWritable, Put)]

      // 如果把在外面new ImmutableBytesWritable()的话就会造成，元组拿到的都是循环中的数据的指向
      // 就会造成list1 里面的数据都是最后一次keyOut.set(put.getRow)的结果了

      for (next <- list) {
        val keyOut = new ImmutableBytesWritable() // 不能放外面，不然出错（无论多少条，只有一条）
        val put = new Put(Bytes.toBytes("spark_tab_part_" + next.getString(0)))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(next.getLong(1)))
        keyOut.set(put.getRow)
        list1 += ((keyOut, put))
      }
      list1.toIterator
    })

    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "user_install_status_spark")
    hbaseConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, classOf[TableOutputFormat[NullWritable]].getName)
    hbaseConf.set("mapreduce.job.output.key.class",classOf[NullWritable].getName)
    hbaseConf.set("mapreduce.job.output.value.class", classOf[Put].getName)

    //这个保存hbase的RDD只发起了5次的连接而不是300次连接，因为在这里给它变成了5个partition，并且没有使用shuffle的方式
    val coalesce: RDD[(ImmutableBytesWritable, Put)] = hbaseRDD.coalesce(5)
    coalesce.saveAsNewAPIHadoopDataset(hbaseConf)
    println(acc.value)
  }
}

