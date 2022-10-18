import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, functions}
import org.apache.spark.sql.functions.unix_timestamp

case class Time(
                 Time_measured: String,
                 Time_hour: Integer,
                 Time_day: Integer,
                 Time_month: Integer,
                 Time_year: Integer,
                 Time_quarter: Integer
               )

object TimeTransformation {

  def main(args: Array[String]) {
    val path = args(0).toString

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("TimeTransformation")
      .enableHiveSupport()
      .getOrCreate;

    import spark.implicits._

    val dateFormat = ("dd/MM/yyyy")

    val timeRDD = spark.read.
      format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(path + "mainDataNorthEngland.csv",
        path + "mainDataScotland.csv",
        path + "mainDataSouthEngland.csv").
      select("year", "count_date", "hour")

    val short = timeRDD.withColumn("count_date", functions.date_format(timeRDD.col("count_date"), dateFormat))

    val result = short.select("count_date", "hour", "year").map {
      case Row(count_date: String, hour: Integer, year: Integer) =>
        Time(
          count_date + "," + hour.toString + ":00",
          hour,
          Integer.parseInt(count_date.substring(0, 2)),
          Integer.parseInt(count_date.substring(3, 5)),
          year,
          (Integer.parseInt(count_date.substring(3, 5)) / 3) + 1
        )
    }

    val result2 =  result.withColumn("Time_measured", unix_timestamp($"Time_measured", "dd/MM/yyyy,HH:mm")
      .cast(TimestampType).as("timestamp")
    )

    val result3 = result2.dropDuplicates("Time_measured")

    result3.write.mode(SaveMode.Append).insertInto("time")
  }

}