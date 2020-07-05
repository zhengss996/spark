package sql

import com.mysql.jdbc.Driver
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlMysqlSession {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions","1")

    val session: SparkSession = SparkSession.builder().config(conf).appName("sparksqlmysqlsession").getOrCreate()

    val data: DataFrame = session.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://nn1.hadoop:3306/python_reptile")
      .option("dbtable", "hainiu_web_seed_externally")
      .option("user", "root")
      .option("password", "12345678").load()

    data.createOrReplaceTempView("temp")
    val row: DataFrame = session.sql("select host from temp")
    row.show()

    //    val context: SparkContext = session.sparkContext
  }
}