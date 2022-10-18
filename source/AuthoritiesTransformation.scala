package com.bigdata.spark

import org.apache.spark.sql.{Row, SaveMode}


case class Authority(
                      local_authority_ons_code: String,
                      local_authority_id: Integer,
                      local_authority_name: String,
                      region_ons_code: String
                    );
case class Region(
                   region_id: Integer,
                   region_name: String,
                   region_ons_code: String
                 );


object AuthoritiesTransformation {
  def main(args: Array[String]): Unit = {

    val path = args(0)

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("AuthoritiesTransformation")
      .master("local")
      //.enableHiveSupport()
      .getOrCreate;

    import spark.implicits._

    val authorities = spark.read.
      format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(path + "authoritiesNorthEngland.csv",
        path + "authoritiesScotland.csv",
        path + "authoritiesSouthEngland.csv").
      map {
        case Row(local_authority_ons_code: String, local_authority_id: Integer, local_authority_name: String, region_ons_code: String) =>
          Authority(local_authority_ons_code,
            local_authority_id,
            local_authority_name,
            region_ons_code)
		}

    val regions = spark.read.
      format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(path + "regionsNorthEngland.csv",
        path + "regionsScotland.csv",
        path + "regionsSouthEngland.csv").
      map {
        case Row(region_id: Integer, region_name: String, region_ons_code: String) =>
          Region(region_id,
            region_name,
            region_ons_code)}.
      withColumnRenamed("region_ons_code", "old_region_ons_code")

    val authoritiesResult = authorities.
      join(regions,
        authorities("region_ons_code") === regions("old_region_ons_code"),
        "left").
      drop("old_region_ons_code")
        .where()

    authoritiesResult.show()

    //authoritiesResult.write.mode(SaveMode.Append).insertInto("authorities")
  }
}