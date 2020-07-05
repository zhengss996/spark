package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkHbaseTablePut {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkhbasetableput").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // 循环每一个元素
    unit.foreach(f => {
      println(f)
      // 获取配置文件
      val hbaseConf: Configuration = HBaseConfiguration.create()
      // 创建连接
      val connection: Connection = ConnectionFactory.createConnection(hbaseConf)
      // 拿到表  （ asInstanceOf：强转）
      val table: HTable = connection.getTable(TableName.valueOf("user_install_status_spark")).asInstanceOf[HTable]

      val put = new Put(Bytes.toBytes("spark_" + f))  // 创建rowkey
      // hbase为什么用 2进制，因为任何语言都可以用，通用语言
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(f))

      table.put(put)
      table.close()
      connection.close()
    })
  }
}
