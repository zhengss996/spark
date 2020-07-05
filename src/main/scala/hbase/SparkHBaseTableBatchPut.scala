package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkHBaseTableBatchPut {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkhbasetablebatchput").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val unit: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)

    unit.foreachPartition(f => {
      val list: List[Int] = f.toList
      println(list)

      import scala.collection.mutable.ListBuffer
      val puts = new ListBuffer[Put]

      for(next <- list){
        val put = new Put(Bytes.toBytes("spark_batch_" + next))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(next))
        puts += put
      }

      val hbaseConf: Configuration = HBaseConfiguration.create()
      val connection: Connection = ConnectionFactory.createConnection(hbaseConf)
      val table: HTable = connection.getTable(TableName.valueOf("user_install_status_spark")).asInstanceOf[HTable]

      // scala 的 list 要转成 Java 的 list
      import scala.collection.convert.wrapAsJava.mutableSeqAsJavaList
//      import scala.collection.convert.wrapAll._
      table.put(puts)
      table.close()
      connection.close()
    })
  }
}
