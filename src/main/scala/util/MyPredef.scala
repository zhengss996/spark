package util

import RDD.SparkWordCount

object MyPredef {
  implicit def deleteHdfs(o:String) = new SparkWordCount(o)
}
