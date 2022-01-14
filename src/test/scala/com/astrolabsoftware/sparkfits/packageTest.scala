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

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.log4j.Level
import org.apache.log4j.Logger

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
  val fn = "src/test/resources/test_file.fits"

  // Test if readfits does nothing :D
  test("Readfits test: Do you send back a DataFrameReader?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
    assert(results.isInstanceOf[DataFrameReader])
  }

  // Test if readfits does nothing :D
  test("Readfits test: Do you yout nickname?") {
    val results = spark.read.format("fits")
    assert(results.isInstanceOf[DataFrameReader])
  }

  // Test DataFrame
  test("DataFrame test: can you really make a DF from the hdu?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
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
        StructField("tata", DoubleType, true),
        StructField("titi", LongType, true),
        StructField("tete", IntegerType, true)
      )
    )

    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .schema(schema)
      .load(fn)
    assert(results.columns.deep == Array("toto", "tutu", "tata", "titi", "tete").deep)
  }

  // Test Data distribution
  test("Data distribution test: Can you count all elements?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn)
    assert(results.select(col("Index")).count().toInt == 20000)
  }

  test("Data distribution test: Can you sum up all elements?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn)
    assert(
      results.select(
        col("Index")).rdd
          .map(_(0).asInstanceOf[Long])
          .reduce(_+_) == 199990000)
  }

  test("Data distribution test: Do you pass over all blocks?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .option("recordlength", 16 * 1024)
      .load(fn)

    val count = results.select(col("Index")).count().toInt
    val count_unique = results.select(col("Index")).distinct().count().toInt

    assert(count == count_unique)
  }

  test("Header printing test") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .option("verbose", true)
      .option("recordlength", 16 * 1024)

    // Finally print the header and exit.
    assert(results.load(fn).isInstanceOf[DataFrame])
  }

  test("Multi files test: Can you read several FITS file?") {
    val fn = "src/test/resources/dir"
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .option("verbose", true)
      .option("recordlength", 16 * 1024)

    assert(results.load(fn).isInstanceOf[DataFrame])
    assert(results.load(fn).count() == 27000)
  }

  test("Multi files test: Can you read several FITS file (glob)?") {
    val fn = "src/test/resources/dir/*.fits"
    val df = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .option("verbose", true)
      .option("recordlength", 16 * 1024)
      .load(fn)

    assert(df.isInstanceOf[DataFrame])
    assert(df.count() == 27000)
  }

  test("Multi files test: Can you read several FITS file (comma-separated)?") {
    val fn = "src/test/resources/dir/test_file.fits,src/test/resources/dir/test_file2.fits"
    val df = spark.read.format("fits")
      .option("hdu", 1)
      .load(fn)

    assert(df.isInstanceOf[DataFrame])
    assert(df.count() == 27000)
  }

  test("Multi files test: Can you detect an error in reading different FITS file [FAILFAST]?") {
    val fn = "src/test/resources/dirNotOk"
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .option("verbose", true)
      .option("mode", "FAILFAST")
      .option("recordlength", 16 * 1024)

      val exception = intercept[AssertionError] {
        results.load(fn).count
      }

    assert(exception.getMessage.contains("different structures"))
  }

  test("Multi files test: Can you read several FITS file (image) discarding empty ones?") {
    val fn = "src/test/resources/dirIm"
    val df = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 2)
      .option("verbose", true)
      .load(fn)

    df.count()

    assert(df.isInstanceOf[DataFrame])
  }

  test("Multi files test: Can you read several FITS file (image) discarding empty ones + set recordLength?") {
    val fn = "src/test/resources/dirIm"
    val df = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 2)
      .option("verbose", true)
      .option("recordlength", 2 * 1024)
      .load(fn)

    df.count()

    assert(df.isInstanceOf[DataFrame])
  }

  test("Multi files test: Can you read several FITS file (image), and fail if there are empty ones??") {
    val fn = "src/test/resources/dirIm/*.fits"
    val df = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 2)
      .option("verbose", true)
      .option("mode", "FAILFAST")
      .load(fn)

    val exception = intercept[AssertionError] {
      df.count()
    }

    assert(exception.getMessage.contains("You are trying to add HDU data with different structures!"))
  }

  test("No file test: Can you detect an error if there is no input FITS file found?") {
    val fn = "src/test/resources/dirfjsdhf"
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .option("verbose", true)
      .option("recordlength", 16 * 1024)

      val exception = intercept[NullPointerException] {
        results.load(fn)
      }

    assert(exception.getMessage.contains("0 files detected"))
  }

  // Test ordering of elements in the DF
  test("Ordering test: Is the first element of the DF correct?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn)
    assert(results.select(col("target")).first.getString(0) == "NGC0000000")
  }

  // Test slash in columns
  test("Slash in names: can you read column names with slash?") {
    val fn = "src/test/resources/colnames_with_slash.fits"
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn)
    assert(results.columns.deep == Array("lsst/u_MEAN", "lsst/g_MEAN", "euclid/VIS_MEAN", "euclid/Y_MEAN").deep)
  }
}
