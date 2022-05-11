/*
 * Copyright 2018 AstroLab Software
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
package com.astrolabsoftware.sparkfits

import com.astrolabsoftware.sparkfits.FitsLib.Fits
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

object ReadFits {
  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession
    .builder()
    .appName("ReadFits")
    .getOrCreate()

  def main(args : Array[String]) = {

    // Loop over the two HDU of the test file
    for (hdu <- 1 to 2) {
      val df = spark.read
        .format("fits")
        .option("hdu", hdu)                 // Index of the HDU
        .option("verbose", true)            // pretty print
        .option("recordlength", 5 * 1024)   // 1 KB per record
        .load(args(0).toString)             // File to load

      println("show>")
      df.show()
      println("printSchema>")
      df.printSchema()

      val count = df.count()
      println("Total rows: " + count.toString)

      println("getHDUFullHeader>")
      val hadoopconf = new Configuration()
      val testpath = new Path(args(0).toString)
      val fitsobj = new Fits(testpath, hadoopconf, 0)
      for (elem <- fitsobj.getHDUFullHeader) {
        println(elem)
      }
    }
  }
}
