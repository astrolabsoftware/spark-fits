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

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import org.apache.log4j.Level
import org.apache.log4j.Logger

import nom.tam.fits.Fits
import nom.tam.fits.BinaryTableHDU

import com.sparkfits.fits._

/**
  * Test class for the package object.
  */
class packageTest extends FunSuite with BeforeAndAfterAll {

  // Set to Level.WARN is you want verbosity
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  private val master = "local[2]"
  private val appName = "sparkfitsTest"

  private var spark : SparkSession = _

  override protected def beforeAll() : Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }
  // END TODO

  // Add more and put a loop for several tests!
  val fn = "src/test/resources/test.fits"

  // Test if readfits does nothing :D
  test("Readfits test: Do you send back a FitsContext?") {
    val results = spark.readfits
    assert(results.isInstanceOf[FitsContext])
  }

  // Test if options grab what we give to it
  test("Option test: can you record a new argument?") {
    val results = spark.readfits.option("toto", "tutu")
    assert(results.extraOptions.contains("toto"))
  }

  // Test if options grab a String
  test("Option test: can you record a String value?") {
    val results = spark.readfits.option("toto", "tutu")
    assert(results.extraOptions("toto").contains("tutu"))
  }

  // Test if options grab a Double
  test("Option test: can you record a Double value?") {
    val results = spark.readfits.option("toto", 3.0)
    assert(results.extraOptions("toto").contains("3.0"))
  }

  // Test if options grab a Long
  test("Option test: can you record a Long value?") {
    val results = spark.readfits.option("toto", 3)
    assert(results.extraOptions("toto").contains("3"))
  }

  // Test if options grab a Boolean
  test("Option test: can you record a Boolean value?") {
    val results = spark.readfits.option("toto", true)
    assert(results.extraOptions("toto").contains("true"))
  }

  // Test yieldRows
  test("yieldRows test: can you spit a bunch of rows?") {
    val f = new Fits(fn)
    val results = spark.yieldRows(f, 1, 0, 10, 100)
    assert(results.size == 10)
  }

  // Test yieldRows
  test("yieldRows test: are you aware of the end of the table data?") {
    val f = new Fits(fn)
    val results = spark.yieldRows(f, 1, 8, 12, 100)
    assert(results.size == 4)
  }

  // Test DataFrame
  test("DataFrame test: can you really make a DF from the hdu?") {
    val results = spark.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .option("printHDUHeader", true)
      .load(fn)
    assert(results.isInstanceOf[DataFrame])
  }

  // Test DataFrame
  test("User schema test: can you really take an external header?") {
    // Specify manually the header
    val schema = StructType(
      List(
        StructField("toto", StringType, true),
        StructField("tutu", FloatType, true),
        StructField("tata", FloatType, true),
        StructField("titi", FloatType, true)
      )
    )

    val results = spark.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .schema(schema)
      .load(fn)
    assert(results.columns.deep == Array("toto", "tutu", "tata", "titi").deep)
  }

  // Test block option
  test("Data distribution test: Can you record the desired number of blocks?") {
    val results = spark.readfits.option("nBlock", 10)
    assert(results.extraOptions("nBlock").contains("10"))
  }

  // Test Data distribution
  test("Data distribution test (user side): can you propagate the number of blocks?") {
    val results = spark.readfits.option("nBlock", 10)
    val f = new Fits(fn)
    val hdu = f.getHDU(1).asInstanceOf[BinaryTableHDU]
    val nrows : Int = hdu.getNRows
    val ncols : Int = hdu.getNCols
    val fileSize : Long = ncols.toLong * nrows.toLong * 8L
    val nBlock : Int = results.getNblocks(fileSize)
    assert(nBlock == 10)
  }

  test("Data distribution test (file side): can you propagate the number of blocks?") {
    val results = spark.readfits
    val f = new Fits(fn)
    val hdu = f.getHDU(1).asInstanceOf[BinaryTableHDU]
    val nrows : Int = hdu.getNRows
    val ncols : Int = hdu.getNCols
    val fileSize : Long = ncols.toLong * nrows.toLong * 8L
    val nBlock : Int = results.getNblocks(fileSize)
    assert(nBlock == 4)
  }

  test("Data distribution test (Big Data side): can you propagate the number of blocks?") {
    val results = spark.readfits

    // Fake a big data set 1 TB
    val fileSize : Long = 1024L * 1024L * 1024L * 1024L
    val nBlock : Int = results.getNblocks(fileSize)
    assert(nBlock == 8192)
  }
}
