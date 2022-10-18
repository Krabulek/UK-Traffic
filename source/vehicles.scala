import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object vehicles {
  def main(args: Array[String]) {

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("vehicles")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate;

    val structureData = Seq(
      Row("pedal_cycles", "moped"),
      Row("two_wheeled_motor_vehicles", "motorcycle"),
      Row("cars_and_taxis","car"),
      Row("buses_and_coaches","bus"),
      Row("lgvs","large goods vehicle"),
      Row("hgvs_2_rigid_axle","truck"),
      Row("hgvs_3_rigid_axle","tank truck"),
      Row("hgvs_4_or_more_rigid_axle","truck"),
      Row("hgvs_3_or_4_articulated_axle","truck"),
      Row("hgvs_5_articulated_axle","special truck"),
      Row("hgvs_6_articulated_axle","special truck")
    )

    val structureSchema = new StructType()
      .add("vehicle_name", StringType)
      .add("vehicle_category", StringType)

    val vehicles = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData),structureSchema)

    vehicles.write.mode(SaveMode.Append).insertInto("vehicles")
  }
}