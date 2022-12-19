import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.functions.{col, desc, from_unixtime, lit}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import java.io.{BufferedReader, FileReader}
import java.sql.{Connection, DriverManager}
import java.nio.file
import scala.io.Source
import scala.util.control.Breaks.break

//import scala.reflect.internal.util.TriState.False

object DbHandler {
  def getConnection: Connection = {
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection("jdbc:postgresql://localhost:5432/Test?user=postgres&password=postgres")
  }
}

object Main extends App{
  //
  var spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Task5")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Loading data from a JDBC source
//  val jdbcDF = spark.read
//    .format("jdbc")
//    .option("url", "jdbc:postgresql://localhost:5432/Test")
//    .option("dbtable", "public.web")
//    .option("user", "postgres")
//    .option("password", "postgres")
//    .load()
//
//  jdbcDF.show()
//  val conn = DbHandler.getConnection
//  val name = "c:/Users/kolyaginkk/IdeaProjects/Task5/inp/yellow_tripdata_2020-01.csv"
//  val tableName = "yellow_tripdata"
//  val rowsInserted = new CopyManager(conn.asInstanceOf[BaseConnection])
//    .copyIn(s"COPY $tableName FROM STDIN (DELIMITER ',',FORMAT csv)",
//      new BufferedReader(new FileReader(name)))
//  println(s"$rowsInserted row(s) inserted for file $name")


  // read CSV file
  val fileName = "c:/Users/kolyaginkk/IdeaProjects/Task5/inp/yellow_tripdata_2020-01.csv"
  val lines: Iterator[String] = Source.fromFile(fileName).getLines()
  var data_rec: List[String] = null;
  var h_rec: List[String] = null;
  var sqlStr = "insert into yellow_tripdata"
  var sqlStr1 = ""
  //val datetime_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  //val groups: Iterator[List[String]] = lines.grouped(100)
  var count: Int = 0;
  var part: Int = 0;
  //val linecount = lines.length
  val linecount = 6405009

  lines.foreach(l => {
    if (count == 0) {
      h_rec = null
      h_rec = l.split(",").toList
      sqlStr+= s" (${h_rec(0)},${h_rec(1)},${h_rec(2)},${h_rec(3)},${h_rec(4)},${h_rec(5)},${h_rec(6)}," +
        s"${h_rec(7)},${h_rec(8)},${h_rec(9)},${h_rec(10)},${h_rec(11)},${h_rec(12)},${h_rec(13)}," +
        s"${h_rec(14)},${h_rec(15)},${h_rec(16)},${h_rec(17)}) values "
      sqlStr1=sqlStr
    }
    else {
      data_rec = null
      data_rec = l.split(",").toList
      sqlStr1 += s"(${data_rec(0)},'${data_rec(1)}','${data_rec(2)}',${data_rec(3)},${data_rec(4)},${data_rec(5)},'${data_rec(6)}'," +
        s"${data_rec(7)},${data_rec(8)},${data_rec(9)},${data_rec(10)},${data_rec(11)},${data_rec(12)},${data_rec(13)}," +
        s"${data_rec(14)},${data_rec(15)},${data_rec(16)},${data_rec(17)}),"
      part += 1
      if ((part==10000) || (count>=linecount)) {
        sqlStr1 = sqlStr1.stripSuffix(",")
        //println(sqlStr1)
        // do database insert
        val conn = DbHandler.getConnection
        try {
          val prep = conn.prepareStatement(sqlStr1)
          prep.executeUpdate
        }
        finally {
          conn.close
        }
        sqlStr1=sqlStr
        part=0
        println(count)
      }
      //if (count==1000) break
    }
    count += 1
  })
}