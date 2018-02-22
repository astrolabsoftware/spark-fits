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
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row
import org.apache.hadoop.io.{ObjectWritable, LongWritable}
import nom.tam.fits.{Fits, BinaryTableHDU}

import org.apache.log4j.Level
import org.apache.log4j.Logger

import com.sparkfits.fits._
import com.sparkfits.FitsFileInputFormat._
import com.sparkfits.FitsSchema_new._
import com.sparkfits.SparkFitsUtil._
import com.sparkfits.FitsBlock._

object ReadFits {
  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession
    .builder()
    .getOrCreate()

  def main(args : Array[String]) = {

    val conf = new Configuration(spark.sparkContext.hadoopConfiguration)

    // test HEADER
    val path = new org.apache.hadoop.fs.Path(args(0).toString)
    val fs = path.getFileSystem(conf)
    // val file = fs.open(path)
    // val header = readHeader(file)
    // val keys = getKeys(header)
    // val values = getValues(header)
    // val names = getNames(header)
    // val comments = getComments(header)
    // header.foreach(println)
    // println(keys.deep)
    // println(values)
    // println(names)
    // println(comments)
    // println(getNRows(header))
    // println(getNCols(header))
    // println(file.getPos)
    // for (i <- 0 to 10) {
    //   file.readFloat
    // }

    val fB = new FitsBlock(path, conf, 1)

    val header = fB.readHeader
    header.foreach(println)

    val startstop = fB.BlockBoundaries
    println(startstop)

    val rowTypes = fB.getRowTypes(header)
    println(rowTypes)

    for (i <- 0 to 10) {
      println(fB.readLine(header))
    }

    val rdd = spark.sparkContext.newAPIHadoopFile(args(0).toString, classOf[FitsFileInputFormat],
      classOf[LongWritable], classOf[Row], conf)

    println("Partitions = " + rdd.getNumPartitions.toString)
    // println("Count = " + rdd.count())
    //
    // val f = new Fits(args(0).toString)
    // val hdu = f.getHDU(1).asInstanceOf[BinaryTableHDU]
    val schema = getSchema(fB)
    println(schema)
    //
    val df = spark.createDataFrame(rdd.map(x=>x._2), schema)
    df.printSchema()
    df.show()
    // df.take(10)
    // for (hdu <- 1 to 2) {
    //   val df = spark.readfits
    //     .option("datatype", "table")
    //     .option("HDU", hdu)
    //     .option("printHDUHeader", true)
    //     .option("nBlock", 100)
    //     .load(args(0).toString)
    //
    //   df.show()
    //   df.printSchema()
    // }
  }
}
