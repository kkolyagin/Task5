import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, max, min, round}
import java.sql.{Connection, DriverManager}
import scala.io.Source
import scala.reflect.io.File
import scala.util.control.Breaks.break

object Main extends App {
  //
  var spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Task5")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val url = "jdbc:postgresql://localhost:5432/Test?user=postgres&password=postgres"

  val countDF = spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", "(SELECT 0 FROM public.yellow_tripdata)  AS t")
    .load()
  val RowCount = countDF.count()
  if (RowCount == 0) ReadCSV //Загрузка файла "yellow_tripdata_2020-01.csv" в таблицу БД

  MakeParquet //Создание витрины и сохранение с перезаписью в таблицу БД  "public.parquet"
  ShowParquet //Вывод на экран таблицы БД  "public.parquet"

  //Загрузка файла "yellow_tripdata_2020-01.csv" в таблицу БД
  def ReadCSV {
    object DbHandler {
      def getConnection: Connection = {
        Class.forName("org.postgresql.Driver")
        DriverManager.getConnection(url)
      }
    }

    // чтение файла CSV из подкаталога inp текущего каталога
    val fileName=File("./inp/yellow_tripdata_2020-01.csv").toAbsolute.toString()

    val lines: Iterator[String] = Source.fromFile(fileName).getLines()
    var data_rec: List[String] = null;
    var h_rec: List[String] = null;
    var sqlStr = "insert into yellow_tripdata"
    var sqlStr1 = ""
    //val datetime_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    //val groups: Iterator[List[String]] = lines.grouped(100)
    var count: Int = 0;
    var part: Int = 0;

    val conn = DbHandler.getConnection
    try {
      lines
        .foreach(l => {
          if (count == 0) {
            h_rec = null
            h_rec = l.split(",").toList
            sqlStr += s" (${h_rec(0)},${h_rec(1)},${h_rec(2)},${h_rec(3)},${h_rec(4)},${h_rec(5)},${h_rec(6)}," +
              s"${h_rec(7)},${h_rec(8)},${h_rec(9)},${h_rec(10)},${h_rec(11)},${h_rec(12)},${h_rec(13)}," +
              s"${h_rec(14)},${h_rec(15)},${h_rec(16)},${h_rec(17)}) values "
            sqlStr1 = sqlStr
          }
          else {
            data_rec = null
            data_rec = l.split(",").toList
            sqlStr1 += s"('${data_rec(0)}','${data_rec(1)}','${data_rec(2)}','${data_rec(3)}','${data_rec(4)}','${data_rec(5)}','${data_rec(6)}'," +
              s"'${data_rec(7)}','${data_rec(8)}','${data_rec(9)}','${data_rec(10)}','${data_rec(11)}','${data_rec(12)}','${data_rec(13)}'," +
              s"'${data_rec(14)}','${data_rec(15)}','${data_rec(16)}','${data_rec(17)}'),"
            part += 1
            if ((part == 10000) || (lines.hasNext == false)) {
              sqlStr1 = sqlStr1.stripSuffix(",").replace("''", "null")
              // do database insert
              conn.prepareStatement(sqlStr1).executeUpdate

              sqlStr1 = sqlStr
              part = 0
              println(count)
            }
          }
          count += 1
        })
    }
    finally {
      conn.close
    }
  }

  def MakeParquet {
    // Loading data from a JDBC source
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "(SELECT date(tpep_pickup_datetime) as date, passenger_count, Total_amount FROM public.yellow_tripdata)  AS t")
//          .option("partitionColumn","date")
//          .option("numPartitions", 51)
//          .option("lowerBound", "2003-01-01") //java.lang.NumberFormatException: For input string: "2003-01-01" ????
//          .option("upperBound", "2021-01-02")
      .load()

    //замена на 0 значений Null в колонке "passenger_count" - для упрощения дальнейших выборок
    val tmpDF = jdbcDF.na.fill(0, Array("passenger_count"))

    //отбор количества поездок по дням
    val tmpAllDF = tmpDF
      .orderBy("date")
      .groupBy("date")
      .agg(count("passenger_count").alias("trip_count_by_day"))

    //отбор количества поездок без пассажиров по дням, а также максимальной и минимальной стоимости поездки в течении дня
    val tmpZeroDF = tmpDF
      .where(col("passenger_count") === 0)
      .orderBy("date")
      .groupBy(col("date").alias("date0"))
      .agg(count("passenger_count").alias("trip_count_zero"),
        max("total_amount").alias("max_pay_zero"),
        min("total_amount").alias("min_pay_zero"))

    //отбор количества поездок с 1 пассажиром по дням, а также максимальной и минимальной стоимости поездки в течении дня
    val tmp1pDF = tmpDF
      .where(col("passenger_count") === 1)
      .orderBy("date")
      .groupBy(col("date").alias("date1"))
      .agg(count("passenger_count").alias("trip_count_1p"),
        max("total_amount").alias("max_pay_1p"),
        min("total_amount").alias("min_pay_1p"))

    //отбор количества поездок с 2 пассажирамм по дням, а также максимальной и минимальной стоимости поездки в течении дня
    val tmp2pDF = tmpDF
      .where(col("passenger_count") === 2)
      .orderBy("date")
      .groupBy(col("date").alias("date2"))
      .agg(count("passenger_count").alias("trip_count_2p"),
        max("total_amount").alias("max_pay_2p"),
        min("total_amount").alias("min_pay_2p"))

    //отбор количества поездок с 3 пассажирамм по дням, а также максимальной и минимальной стоимости поездки в течении дня
    val tmp3pDF = tmpDF
      .where(col("passenger_count") === 3)
      .orderBy("date")
      .groupBy(col("date").alias("date3"))
      .agg(count("passenger_count").alias("trip_count_3p"),
        max("total_amount").alias("max_pay_3p"),
        min("total_amount").alias("min_pay_3p"))

    //отбор количества поездок с 3 пассажирамм по дням, а также максимальной и минимальной стоимости поездки в течении дня
    val tmp4pDF = tmpDF
      .where(col("passenger_count") >= 4)
      .orderBy("date")
      .groupBy(col("date").alias("date4"))
      .agg(count("passenger_count").alias("trip_count_4p_plus"),
        max("total_amount").alias("max_pay_4p_plus"),
        min("total_amount").alias("min_pay_4p_plus"))

    //сборка витрины из предыдущих выборок
    val itogDF = tmpAllDF
      .join(tmpZeroDF, tmpAllDF("date") === tmpZeroDF("date0"), "left")
      .join(tmp1pDF, tmpAllDF("date") === tmp1pDF("date1"), "left")
      .join(tmp2pDF, tmpAllDF("date") === tmp2pDF("date2"), "left")
      .join(tmp3pDF, tmpAllDF("date") === tmp3pDF("date3"), "left")
      .join(tmp4pDF, tmpAllDF("date") === tmp4pDF("date4"), "left")
      .withColumn("percentage_zero", round((col("trip_count_zero") / col("trip_count_by_day") * 100), 2))
      .withColumn("percentage_1p", round((col("trip_count_1p") / col("trip_count_by_day")) * 100, 2))
      .withColumn("percentage_2p", round((col("trip_count_2p") / col("trip_count_by_day")) * 100, 2))
      .withColumn("percentage_3p", round((col("trip_count_3p") / col("trip_count_by_day")) * 100, 2))
      .withColumn("percentage_4p_plus", round((col("trip_count_4p_plus") / col("trip_count_by_day")) * 100, 2))
      .select("date", "percentage_zero", "percentage_1p", "percentage_2p", "percentage_3p", "percentage_4p_plus"
        , "max_pay_zero", "max_pay_1p", "max_pay_2p", "max_pay_3p", "max_pay_4p_plus"
        , "min_pay_zero", "min_pay_1p", "min_pay_2p", "min_pay_3p", "min_pay_4p_plus")
      .orderBy("date")

    //сохранение с перезаписью в таблицу БД  "public.parquet"
    itogDF.write
      .format("jdbc")
      .mode("overwrite")
      .option("url", url)
      .option("dbtable", "public.parquet")
      .save()
  }

  //вывод таблицы БД  "public.parquet" на экран
  def ShowParquet {
    //вывод на экран таблицы "public.parquet"
    val parquetDF = spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "(SELECT * FROM public.parquet)  AS t")
      .load()
    println(parquetDF.show(parquetDF.count().toInt))
  }
}