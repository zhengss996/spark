package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, HTable}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.serializer.KryoSerializer

class SparkHBaseBulkLoad
object SparkHBaseBulkLoad {
  def main(args: Array[String]): Unit = {
    import util.MyPredef.deleteHdfs
//    val hdfsOutPath = "C:\\Users\\song\\Desktop\\output\\sparkhbasdebulk"
    val hdfsOutPath = args(1)
    hdfsOutPath.deletePath

//    val orcPath = "D:\\tmp\\spark\\part-r-00001"
    val orcPath = args(0)

    val conf: SparkConf = new SparkConf()
    conf.setAppName("sparkhbasescan")
//    conf.setMaster("local[*]")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)

    val sc = new SparkContext(conf)
    val hivec = new HiveContext(sc)
    val df: DataFrame = hivec.read.orc(orcPath)
    val hbaseRDD: RDD[(ImmutableBytesWritable, KeyValue)] = df.limit(10).rdd.mapPartitions(rows => {
      val list: List[Row] = rows.toList
      import scala.collection.mutable.ListBuffer
      val list1 = new ListBuffer[(ImmutableBytesWritable, KeyValue)]

      for (r <- list) {
        val rk = new ImmutableBytesWritable()
        rk.set(Bytes.toBytes("spark_bulk_" + r.getString(1)))
        val keyValue = new KeyValue(rk.get(), Bytes.toBytes("cf"), Bytes.toBytes("country"), Bytes.toBytes(r.getString(4)))
        list1 += ((rk, keyValue))
      }

      list1.toIterator
    }).sortByKey()

    val hbaseConf: Configuration = HBaseConfiguration.create()
    val job: Job = Job.getInstance(hbaseConf)

    // 本地运行需要开启管理员权限、或者注释下面三句
    val connection: Connection = ConnectionFactory.createConnection(hbaseConf)
    val table: HTable = connection.getTable(TableName.valueOf("user_install_status_spark")).asInstanceOf[HTable]
    HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor, table.getRegionLocator)

    hbaseRDD.saveAsNewAPIHadoopFile(hdfsOutPath,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],hbaseConf)

    // 集群运行添加下面三句
    val loader = new LoadIncrementalHFiles(hbaseConf)
    val admin: Admin = connection.getAdmin
    loader.doBulkLoad(new Path(hdfsOutPath),admin,table,table.getRegionLocator())
  }
}
