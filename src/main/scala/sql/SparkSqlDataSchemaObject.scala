package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


class HainiuSqlData(var line:String){
  def getLine:String = {
    this.line
  }
  def setLine(line:String) = {
    this.line = line
  }
}
object SparkSqlDataSchemaObject {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparksqldataschema").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val jsonPath = "D:\\tmp\\spark\\spark_sql_json.txt"
    val map: RDD[HainiuSqlData] = sc.textFile(jsonPath).map(f => new HainiuSqlData(f))

    val sqlc = new SQLContext(sc)
    val df: DataFrame = sqlc.createDataFrame(map,classOf[HainiuSqlData])
    df.printSchema()
    df.createOrReplaceTempView("hainiu_table")  // 注册成一张表

    val sql: DataFrame = sqlc.sql("select line as lineT from hainiu_table where line like \"%gameloft%\"")
    sql.printSchema()
    sql.show()

    val value: RDD[(String, Int)] = sql.rdd.map(f => (s"json_str:${f.getString(0)}",1))
    val jsonMap: collection.Map[String, Int] = value.collectAsMap()
    jsonMap.foreach((f:(String,Int)) => {
      println(s"${f._1}\t${f._2}")
    })
  }
}