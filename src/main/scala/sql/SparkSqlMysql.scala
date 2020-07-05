package sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlMysql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparksqlmysql").setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions","1")

    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    val jdbc: DataFrame = sqlc.jdbc("jdbc:mysql://nn1.hadoop:3306/python_reptile?user=root&password=12345678","hainiu_web_seed_internally")
    val jdbc1: DataFrame = sqlc.jdbc("jdbc:mysql://nn1.hadoop:3306/python_reptile?user=root&password=12345678","hainiu_web_seed_externally")
    jdbc.createOrReplaceTempView("aa")
    jdbc1.createOrReplaceTempView("bb")
    val join: DataFrame = sqlc.sql("select * from aa a inner join bb b on a.md5=b.md5 limit 100")
    join.show()
  }
}