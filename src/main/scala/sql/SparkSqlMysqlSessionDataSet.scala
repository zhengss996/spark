package sql

import com.mysql.jdbc.Driver
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSqlMysqlSessionDataSet {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions","1")

    val session: SparkSession = SparkSession.builder().config(conf).appName("sparksqlmysqlsessiondataset").getOrCreate()

    val data: DataFrame = session.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://nn1.hadoop:3306/python_reptile")
      .option("dbtable", "hainiu_web_seed_externally")
      .option("user", "root")
      .option("password", "12345678").load()

    data.createOrReplaceTempView("temp")
    val row: DataFrame = session.sql("select host from temp")

    import session.implicits._
    val value: Dataset[HainiuSparkDataSet] = row map {
      case host: Row => HainiuSparkDataSet("hainiu_" + host.mkString)
      case _ => HainiuSparkDataSet("")
    }
    //这个show是ds的，打印的是HainiuSparkDataSet类型的数据
    value.show()

    val frame: DataFrame = value.rdd.toDF()
    frame.printSchema()
    //这个show是df的，打印的是row类型的数据
    frame.show()

  }
}
case class HainiuSparkDataSet(val host:String)
