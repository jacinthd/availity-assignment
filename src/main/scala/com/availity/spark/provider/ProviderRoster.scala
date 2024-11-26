package com.availity.spark.provider

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DateType, StringType, StructType}
import org.apache.spark.sql.functions.{array, avg, col, collect_list, concat_ws, count, date_format, lit, month, sum, to_date}

object ProviderRoster  {

  def main(args: Array[String]): Unit = {
    // run this using sbt compile, then sbt run
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("CSV Reader")
      .master("local[*]")
      .getOrCreate()

    val providersPath = "data/providers.csv"
    val visitsPath = "data/visits.csv"

    val providersDf: DataFrame = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema", "true")
      .csv(providersPath)

    val visitsDf: DataFrame = spark.read
      .option("inferSchema", "true")
      .csv(visitsPath)
      .toDF(Seq("visit_id", "provider_id", "date_visit"): _*)

    val providersDfWithName: DataFrame = providersDf.withColumn(
      "name",
      concat_ws(" ", col("first_name"), col("middle_name"), col("last_name"))
    ).drop("first_name", "middle_name", "last_name")

    // it is not clear from the question if monthly visits by year-month or just month are needed
    // Assuming it is year-month, since avg by month makes more sense than sum by month because there will be multiple values by year
    val monthlyVisitsDf: DataFrame = visitsDf
      .withColumn("month", date_format(to_date(col("date_visit")), "yyyy-MM"))
      .groupBy("provider_id", "year_month")
      .agg(count("visit_id").alias("visit_count"))

    val visitsProviderDf: DataFrame = monthlyVisitsDf
      .groupBy("provider_id")
      .agg(sum("visit_count").alias("visit_count"))

    val visitsProviderJoinedDf: DataFrame = visitsProviderDf
      .join(providersDfWithName, Seq("provider_id"), "left")
      .select("provider_id", "name", "provider_specialty", "visit_count")
      .withColumn("provider_specialty_copy", col("provider_specialty"))

    val jsonOutputPath1 = "output/total_visits_by_provider_by_month"
    val jsonOutputPath2 = "output/total_visits_by_provider"

    monthlyVisitsDf
      .coalesce(1)
      .write
      .mode("overwrite")
      .json(jsonOutputPath1)

    visitsProviderJoinedDf
      .coalesce(1)
      .write
      .partitionBy("provider_specialty_copy")
      .mode("overwrite")
      .json(jsonOutputPath2)

    spark.stop()
  }
}
