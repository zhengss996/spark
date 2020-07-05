package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountSort {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkwordcountsort").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("debug")

    import util.MyPredef.deleteHdfs
    val outPath = "C:\\Users\\song\\Desktop\\output\\sparkwordcountsort"
    outPath.deletePath

    val line: RDD[String] = sc.textFile("D:\\tmp\\spark\\word.txt", 10)
    println(line.partitions.length)

    // spark中的sort是个全局排序的sort，因为其已经是一个RangePartitioner，而mr想全局排序必须自己实现一个Partitioner
    val sort: RDD[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2)
    sort.saveAsTextFile(outPath)

    // 打印rdd的debug信息，可以方便的查看rdd的依赖，从而可以看到哪一步产生了shuffle
    println(sort.toDebugString)

    sc.stop()
  }
}
