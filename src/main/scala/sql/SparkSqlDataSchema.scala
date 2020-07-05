package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlDataSchema {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparksqldataschema").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val jsonPath = "D:\\tmp\\spark\\spark_sql_json.txt"
    val map: RDD[Row] = sc.textFile(jsonPath).map(f => RowFactory.create(f))

    import scala.collection.mutable.ArrayBuffer
    val fields = new ArrayBuffer[StructField]()
    fields += DataTypes.createStructField("line", DataTypes.StringType, true)
    val tableSchema: StructType = DataTypes.createStructType(fields.toArray)

    val sqlc = new SQLContext(sc)
    val df: DataFrame = sqlc.createDataFrame(map,tableSchema)
    df.printSchema()
    val filter: Dataset[Row] = df.filter(df.col("line").like("%gameloft%"))
    filter.printSchema()
    filter.show()
    val l: Long = filter.count()
    println(s"like count:${l}")
  }
}
