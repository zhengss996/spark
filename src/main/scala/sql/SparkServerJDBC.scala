package sql

import java.sql.{Connection, DriverManager, ResultSet, Statement}

object SparkServerJDBC {
  def main(args: Array[String]): Unit = {

    classOf[org.apache.hive.jdbc.HiveDriver]
    val connection: Connection = DriverManager.getConnection("jdbc:hive2://nn1.hadoop:20000/class","","")
    try{
      val statement: Statement = connection.createStatement()
      statement.execute("set spark.sql.shuffle.partitions=20")
      val sql:String = """
                         |select count(1) as c
                         | from
                         |(select count(1) from user_install_status_limit group by aid) a
                       """.stripMargin
      val set: ResultSet = statement.executeQuery(sql)
      while (set.next()){
        println(set.getLong("c"))  // 字段
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      connection.close()
    }
  }
}