package com.bigdata.spark

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions._

object WeatherTransformation {
  def main(args: Array[String]) {

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("WeatherTransformation")
      //      .master("local")
      .enableHiveSupport()
      .getOrCreate;

    val schema =  DataTypes.createStructType(Array(
      DataTypes.createStructField("toDrop1",DataTypes.StringType,false),
      DataTypes.createStructField("weather_id",DataTypes.StringType,false),
      DataTypes.createStructField("weather_condition",DataTypes.StringType,false)
    ))

    val path = args(0)
    val weatherConditionCSV = spark.read.
      format("csv").
      option("header", false).
      option("delimiter", ":").
      schema(schema).
      load(path + "weather.txt").
      select("weather_id", "weather_condition").
      dropDuplicates("weather_condition").
      filter(col("weather_condition").isNotNull).
      filter(!col("weather_condition").contains("Unknown")).
      filter(!col("weather_condition").contains("null")).
      toDF("weather_id", "weather_condition")

    val w = Window.orderBy("weather_condition")
    val weatherCondition = weatherConditionCSV
      .withColumn("weather_condition", trim(col("weather_condition")))
      .withColumn("weather_id", row_number().over(w))

    //    weather.show()
    weatherCondition.write.mode(SaveMode.Append).insertInto("weather")
  }
}