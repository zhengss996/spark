package sql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlHiveOrc {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparksqljson").setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions","1")
    val sc = new SparkContext(conf)

    val orcPath = "D:\\tmp\\spark\\part-r-00001"

    val hivec = new HiveContext(sc)
    val df: DataFrame = hivec.read.orc(orcPath)
    df.printSchema()

    df.select(df.col("country")).show(5)
    df.select(df.col("country").as("local")).show(5)
    val count: DataFrame = df.groupBy("country").count()
    count.printSchema()

    val select: DataFrame = count.select(df.col("country"),count.col("count").alias("num"))
    select.printSchema()
    val selectCount: Dataset[Row] = select.filter(select.col("num").lt(3000)).limit(10)

    //默认级别是MEMORY_AND_DISK，而RDD的默认级别是MEMORY_ONLY，所以使用的时候要注意区别
    val cache:Dataset[Row] = selectCount.persist()

    cache.write.mode(SaveMode.Overwrite).format("orc").save("C:\\Users\\song\\Desktop\\output\\sqlhiveorc2orc")
    cache.write.mode(SaveMode.Overwrite).format("json").save("C:\\Users\\song\\Desktop\\output\\sqlhiveorc2json")
  }
}