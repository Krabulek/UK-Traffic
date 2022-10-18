package main

import org.apache.spark.sql.SaveMode


object ukRoads {

  def main(args: Array[String]) {

    val path = args(0).toString()

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("RoadsTransformation")
      .enableHiveSupport()
      //.master("local")
      .getOrCreate;

    val roadsDFCsv = spark.read.
      format("org.apache.spark.csv").
      option("header", true).
      option("InferSchema", true).
      csv(path + "mainDataNorthEngland.csv",
        path + "mainDataScotland.csv",
        path + "mainDataSouthEngland.csv").
      select("road_name", "road_category", "road_type")


    val distinctRoadsDFCsv = roadsDFCsv.dropDuplicates("road_name", "road_category", "road_type")

    //distinctRoadsDFCsv.show()

    distinctRoadsDFCsv.write.mode(SaveMode.Append).insertInto("roads")

  }


}

