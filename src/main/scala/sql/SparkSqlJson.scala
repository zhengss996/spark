package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlJson {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlJson").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val jsonPath = "D:\\tmp\\spark\\spark_sql_json.txt"
    val sqlc = new SQLContext(sc)
    // df 能注册一张表，还能查
    val df: DataFrame = sqlc.read.json(jsonPath)
    // 打印表结构
    df.show()
    // 打印表树状结构
    df.printSchema()
    // 打印国家名 这列
    df.select(df.col("country")).show()
    //                                                              plus : +1       alias : 别名
    df.select(df.col("country"), df.col("num").plus(1).alias("num_add")).show()
    // 查找 num 值 < 2 的值
    df.filter(df.col("num").lt(2)).show()
    // select country, count(1) from table group by country
    val count: DataFrame = df.groupBy("country").count()
    count.show()
    count.printSchema()

    val rdd: RDD[Row] = count.rdd
    import util.MyPredef.deleteHdfs
    val savePath = "C:\\Users\\song\\Desktop\\output\\sqlJjon"
    savePath.deletePath
    rdd.saveAsTextFile(savePath)
  }
}