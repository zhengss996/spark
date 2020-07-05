package util

import RDD.MapJoin
import hbase.SparkHBaseBulkLoad
import org.apache.hadoop.util.ProgramDriver

object Driver {
  def main(args: Array[String]): Unit = {
    val driver = new ProgramDriver
    driver.addClass("mapjoin", classOf[MapJoin], "mapjoin任务")
    driver.addClass("sparkhbaseload",classOf[SparkHBaseBulkLoad],"hbasebulkload任务")
    driver.run(args)
  }
}
