package com.bigdata.spark

import java.sql.Timestamp

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{concat, date_format, lit, unix_timestamp}
import org.apache.spark.sql.types.TimestampType


case class Fact (
              local_authority_ons_code: String,
              time_measured: Timestamp,
              road_name: String,
              vehicle_name: String,
              num_vehicles: Integer
            )

object FactsTransformation {

  def main(args: Array[String]) {

    val path = args(0)

    val spark = org.apache.spark.sql.SparkSession.builder.
      appName("FactsTransformation").
      //master("local").
      enableHiveSupport().
      getOrCreate;

    import spark.implicits._

    val factsDFcsv = spark.read.
      format("org.apache.spark.csv").
      option("header", true).
      option("InferSchema", true).
      csv(
        path + "mainDataNorthEngland.csv",
        path + "mainDataScotland.csv",
        path + "mainDataSouthEngland.csv"
      ).
      select("count_date",
        "hour",
        "local_authoirty_ons_code",
        "road_name",
        "pedal_cycles",
        "two_wheeled_motor_vehicles",
        "cars_and_taxis",
        "buses_and_coaches",
        "lgvs",
        "hgvs_2_rigid_axle",
        "hgvs_3_rigid_axle",
        "hgvs_4_or_more_rigid_axle",
        "hgvs_3_or_4_articulated_axle",
        "hgvs_5_articulated_axle",
        "hgvs_6_articulated_axle"
      )

    val factsDF = factsDFcsv.
      withColumn("count_date",
        date_format(factsDFcsv.col("count_date"), "dd/MM/yyyy")
      ).
      withColumn( "time_measured",
        concat($"count_date", lit(","), $"hour",lit(":00") )
      ).
      withColumn("time_measured",
        unix_timestamp($"time_measured", "dd/MM/yyyy,HH:mm")
          .cast(TimestampType).as("timestamp")
      ).
      withColumnRenamed("local_authoirty_ons_code", "local_authority_ons_code").
      drop("count_date", "hour")


    val facts = factsDF.flatMap(
      row => Array(
        Fact(row.getString(0), row.getTimestamp(13), row.getString(1), "pedal_cycles", row.getInt(2)),
        Fact(row.getString(0), row.getTimestamp(13), row.getString(1), "two_wheeled_motor_vehicles", row.getInt(3)),
        Fact(row.getString(0), row.getTimestamp(13), row.getString(1), "cars_and_taxis", row.getInt(4)),
        Fact(row.getString(0), row.getTimestamp(13), row.getString(1), "buses_and_coaches", row.getInt(5)),
        Fact(row.getString(0), row.getTimestamp(13), row.getString(1), "lgvs", row.getInt(6)),
        Fact(row.getString(0), row.getTimestamp(13), row.getString(1), "hgvs_2_rigid_axle", row.getInt(7)),
        Fact(row.getString(0), row.getTimestamp(13), row.getString(1), "hgvs_3_rigid_axle", row.getInt(8)),
        Fact(row.getString(0), row.getTimestamp(13), row.getString(1), "hgvs_4_or_more_rigid_axle", row.getInt(9)),
        Fact(row.getString(0), row.getTimestamp(13), row.getString(1), "hgvs_3_or_4_articulated_axle", row.getInt(10)),
        Fact(row.getString(0), row.getTimestamp(13), row.getString(1), "hgvs_5_articulated_axle", row.getInt(11)),
        Fact(row.getString(0), row.getTimestamp(13), row.getString(1), "hgvs_6_articulated_axle", row.getInt(12))
      )
    )
    facts.write.mode(SaveMode.Overwrite).insertInto("facts")

  }

}
