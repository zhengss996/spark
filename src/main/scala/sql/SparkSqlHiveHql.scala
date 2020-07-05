package sql

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlHiveHql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparksqljson").setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions","1")
    conf.set("spark.io.compression.codec","snappy")
    val sc = new SparkContext(conf)

    val orcPath = "file:/D:/tmp/spark/part-r-00001"

    val hivec = new HiveContext(sc)
    hivec.sql("create database if not exists zhengsongsong")
    hivec.sql("use zhengsongsong")

    val createTableSql = """
                           |CREATE TABLE if not exists `user_install_status`(
                           |  `aid` string COMMENT 'from deserializer',
                           |  `pkgname` string COMMENT 'from deserializer',
                           |  `uptime` bigint COMMENT 'from deserializer',
                           |  `type` int COMMENT 'from deserializer',
                           |  `country` string COMMENT 'from deserializer',
                           |  `gpcategory` string COMMENT 'from deserializer')
                           |ROW FORMAT SERDE
                           |  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
                           |STORED AS INPUTFORMAT
                           |  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
                           |OUTPUTFORMAT
                           |  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
                           |TBLPROPERTIES('orc.compress'='SNAPPY','orc.create.index'='true')
                         """.stripMargin

    hivec.sql(createTableSql)
    hivec.sql(s"load data local inpath '${orcPath}' into table user_install_status")

    val df: DataFrame = hivec.sql("select * from user_install_status limit 10")
    df.printSchema()
    df.show()

    val sql: DataFrame = hivec.sql(
      """
        |select concat(country,"\t",num) as concatString from
        |(select country,count(1) as num from user_install_status group by country) a
        |where a.num < 3000 limit 10
      """.stripMargin)
    sql.printSchema()
    val cache: DataFrame = sql.cache()
    cache.show()
    cache.write.mode(SaveMode.Overwrite).format("text").save("C:\\Users\\Song\\Desktop\\output\\sqlhiveorc2text")
  }
}