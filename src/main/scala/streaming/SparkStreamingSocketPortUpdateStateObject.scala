package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

case class StreamingStatusValue(var value:Int,var isUpdate:Boolean = false)

object SparkStreamingSocketPortUpdateStateObject {
  def main(args: Array[String]): Unit = {

    val checkPointPath = "C:\\Users\\song\\Desktop\\output\\sparkstreamingsocketportupdatestateobject"

    val func: () => StreamingContext = () => {
      val conf: SparkConf = new SparkConf().setAppName("sparkstreamingsocketportupdatestateobject").setMaster("local[2]")
      val streamingContext = new StreamingContext(conf, Durations.seconds(5))
      streamingContext.checkpoint(checkPointPath)
      //这里与要恢复的冲突了
      val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("nn1.hadoop", 6666)

      //transform和foreachRDD都可以进行RDD转换，比如RDD转成DS或DF进行spark-sql的操作，这时就方便了
      //但是要记住transform是转换操作，而foreachRDD是动作，这两个的共同特点是可让写spark-streaming程序像写spark-core一样
      //可以有本地端和集群运行的（比如算子中的函数）
      //DStream自己也有和RDD一样的filter,map算子，并且有自己独有的比如updateStateByKey，windows这样的算子
      //但是转换的函数的返回值是一个DStream，所以不像transform和foreachRDD的函数可以得到原始的RDD，所以普通的DStream算子无法进行spark-sql的运算
      //注意：只有这两个transform和foreachRDD算子里的函数代码分"本地"和"集群"运算的，而DStream其它算子的函数不分本地和集群的，因为其它都是集群的

      lines.transform(r => {
        val flatMap: RDD[String] = r.flatMap(_.split(" "))
        val mapToPair: RDD[(String, StreamingStatusValue)] = flatMap.map((_, StreamingStatusValue(1)))
        val reduceByKey: RDD[(String, StreamingStatusValue)] = mapToPair.reduceByKey((a,b) => StreamingStatusValue(a.value + b.value))

        //将RDD转成spark-sql进行开发  （伪代码）
        //        import sparkSession.implicit._
        //        val df:DataFrame = reduceByKey.toDF
        //        df.createOrReplaceTempView("test")
        //        val df1:DataFrame = sparkSession.sql("select * from test")
        //            ...
        //        df1.rdd
        reduceByKey
      })

      val flatMap: DStream[String] = lines.flatMap(_.split(" "))
      val mapToPair: DStream[(String, StreamingStatusValue)] = flatMap.map((_, StreamingStatusValue(1)))
      val reduceByKey: DStream[(String, StreamingStatusValue)] = mapToPair.reduceByKey((a,b) => StreamingStatusValue(a.value + b.value))

      val updateStateByKey: DStream[(String, StreamingStatusValue)] = reduceByKey.updateStateByKey((a: Seq[StreamingStatusValue], b: Option[StreamingStatusValue]) => {
        var total = 0
        for (i <- a) {
          total += i.value
        }
        val last: StreamingStatusValue = if (b.isDefined) b.get else StreamingStatusValue(0)

        if(a.size != 0){
          last.isUpdate = true
        }else{
          last.isUpdate = false
        }
        val now = total + last.value
        last.value = now
        Some(last)
      })

      updateStateByKey.foreachRDD((r, t) => {
        val filter: RDD[(String, StreamingStatusValue)] = r.filter(_._2.isUpdate)

        println(s"count time:${t},${r.collect().toList}")
        println(s"filter data:${t},${filter.collect().toList}")
      })
      streamingContext
    }

    val strc:StreamingContext =StreamingContext.getOrCreate(checkPointPath, func)
    strc.start()
    strc.awaitTermination()
  }
}