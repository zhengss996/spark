package RDD

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

object SparkBroadCastDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkbroadcastdemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val arr = Array(1, 2, 3, 4, 5)
    val par: RDD[Int] = sc.parallelize(arr, 5)

    val broad: Broadcast[Array[Int]] = sc.broadcast(arr)
    val acc: Accumulator[Int] = sc.accumulator(0)

    val reduceResult: Int = par.reduce((a, b) => {
      val broadT: Array[Int] = broad.value
      // 这是使用广播变量
      println(s"broad:${broadT.toList}")
      // 这是使用外部变量
      println(s"arr:${arr.toList}")
      //累加结果只能使用累加器，不能使用外部变量，因为在分布式环境下，外部变量是不同步的
      //累加器在使用的时候需要注意，如果RDD有N个Action的时候那累加器N次
      acc.add(1)
      a + b
    })

    println(reduceResult)
    println(acc)
  }
}
