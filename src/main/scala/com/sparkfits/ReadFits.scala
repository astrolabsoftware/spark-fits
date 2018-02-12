package com.sparkfits

import org.apache.spark.sql.SparkSession

import org.apache.log4j.Level
import org.apache.log4j.Logger

import com.sparkfits.fits._

object ReadFits {
  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession
    .builder()
    .getOrCreate()

  def main(args : Array[String]) = {

    for (hdu <- 1 to 2) {
      val df = spark.readfits
        .option("datatype", "table")
        .option("HDU", hdu)
        .option("printHDUHeader", true)
        .load(args(0).toString)

      df.show()
      df.printSchema()
    }
  }
}
