package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountSecondarySort {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkwordcountsecondarysort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val line: RDD[String] = sc.textFile("D:\\tmp\\spark\\sort_secondary.txt")
    val mapRDD: RDD[(SecondarySortKey, String)] = line.filter(f => {
      if (f.contains("(") && f.contains(")") && !f.equals("")) {
        true
      } else {
        false
      }
    }).map(f => {
      val strings: Array[String] = f.split(",")
      val word: String = strings(0).substring(1)
      val num: String = strings(1).substring(0, strings(1).length - 1)
      //                    入参                返回值
      (new SecondarySortKey(word, num.toInt), s"${word}\t${num}")
    })
    val sortBykeyRDD: RDD[(SecondarySortKey, String)] = mapRDD.sortBy(_._1, false)
    println(sortBykeyRDD.toDebugString)
    //            取值数量 limit
    sortBykeyRDD.take(100).foreach(f => println(s"${f._1.word} ${f._1.count}"))
  }
}
