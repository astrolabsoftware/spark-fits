package sparkfits

import org.apache.spark.sql.SparkSession

import org.apache.log4j.Level
import org.apache.log4j.Logger

import sparkfits.fits._

object ReadFits {
  val spark = SparkSession
    .builder()
    .getOrCreate()

  def main(args : Array[String]) = {

    val df = spark.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .option("printHDUHeader", true)
      .load(args(0).toString)

    df.show()
    df.printSchema()
  }
}
