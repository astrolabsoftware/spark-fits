/*
 * Copyright 2018 Julien Peloton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sparkfits

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

    // Loop over the two HDU of the test file
    for (hdu <- 1 to 2) {
      val df = spark.readfits
        .option("datatype", "table")        // Binary table
        .option("HDU", hdu)                 // Index of the HDU
        .option("printHDUHeader", true)     // pretty print
        .option("recordLength", 128 * 1024) // 128 KB per record
        .load(args(0).toString)             // File to load

      df.show()
      df.printSchema()

      // val c = df.select(col("Index")).count().toInt
      // val c_d = df.select(col("Index")).distinct.count().toInt
      // val s = df.select(col("Index")).rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)

      // println("Total count: " + c.toString)
      // println("Unique count: " + c_d.toString)
      // println("Total sum: " + s.toString)

    }
  }
}
