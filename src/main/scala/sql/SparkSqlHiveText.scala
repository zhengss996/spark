package sql

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlHiveText {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparksqljson").setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions","1")
    val sc = new SparkContext(conf)

    val orcPath = "D:\\tmp\\spark\\part-r-00001"

    val hivec = new HiveContext(sc)
    val df: DataFrame = hivec.read.orc(orcPath)
    df.createOrReplaceTempView("user_install_status")
    val sql:DataFrame = hivec.sql(
      """
        |select concat(country,"\t",num) as concatString from
        |(select country,count(1) as num from user_install_status group by country) a
        |where a.num < 3000 limit 10
      """.stripMargin)
    sql.printSchema()

    val cache: DataFrame = sql.cache()
    cache.show()
    cache.write.mode(SaveMode.Overwrite).format("text").save("C:\\Users\\song\\Desktop\\output\\sqlhiveorc2text")
  }
}