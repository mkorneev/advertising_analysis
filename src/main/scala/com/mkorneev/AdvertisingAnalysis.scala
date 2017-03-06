package com.mkorneev

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions._

object AdvertisingAnalysis {

  private val config = ConfigFactory.load()

  object Config {
    val comagicMappingPath: String = config.getString("comagic_mapping_path")
    val comagicPath: String = config.getString("comagic_path")
    val bookingsPath: String = config.getString("bookings_path")
    val dealsPath: String = config.getString("deals_path")
    val outputPath: String = config.getString("output_path")
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("AdvertisingAnalysis")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    def loadExcel(location: String, skipRows: Int = 0)(implicit spark: SparkSession) = {
      spark.read
        .format("com.crealytics.spark.excel")
        .option("location", location)
        //.option("sheetName", "0")
        .option("skipRows", skipRows)
        .option("useHeader", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "false")
        .option("addColorColumns", "false")
        .load()
    }

    def cleanPhoneNumber(col: ColumnName) = {
      substring(regexp_replace(col, raw"\D", ""), -10, 10)  // take last 10 digits
    }

    val comagic_mapping = loadExcel(Config.comagicMappingPath)

    val comagic = loadExcel(Config.comagicPath)
      //  .limit(1000).cache
      .select("Дата и время", "Номер абонента", "Рекламная кампания")
      .withColumn("Дата и время", unix_timestamp($"Дата и время", "dd/MM/yyyy HH:mm").cast("timestamp"))
      .withColumn("Номер абонента", cleanPhoneNumber($"Номер абонента"))
      .join(comagic_mapping, Seq("Рекламная кампания"), "left_outer")

    comagic.show(10, false)

    //TableChart(comagic.groupBy("Подрядчик").count().sort($"count".desc))

    val bookings = loadExcel(Config.bookingsPath)
      //  .limit(1000).cache
      .withColumn("Адрес", concat($"Объект строительства", lit(" "), $"Объект недвижимости"))
      .withColumnRenamed("Телефон", "Телефон брони")
      .select("Проект", "Дата операции", "Адрес", "Телефон брони")

    bookings.show(3, false)

    val deals = loadExcel(Config.dealsPath, skipRows = 5)
      //  .limit(1000).cache
      .withColumn("Дата и время сделки", unix_timestamp($"Период", "dd.MM.yyyy HH:mm:ss").cast("timestamp"))
      .withColumn("Адрес", concat(
        $"`Сделка.Документ брони.Объект строительства`", lit(" "),
        $"`Сделка.Документ брони.Объект недвижимости`"))
      .withColumn("Телефон клиента", cleanPhoneNumber($"Телефон клиента"))
      .select("Дата и время сделки", "Адрес", "Телефон клиента")

      .join(bookings, Seq("Адрес"), "left_outer")
      .join(comagic, $"Телефон клиента" === $"Номер абонента", "left_outer")

    deals.show(3, false)

    //TableChart(deals.groupBy("ЖК").count().sort($"count".desc))

    deals.coalesce(1)
      .write.format("csv")
      .option("header", "true")
      .save(Config.outputPath)

  }
}

