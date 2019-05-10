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

import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
  * Test class for the package object.
  */
class ReadFitsTest extends FunSuite with BeforeAndAfterAll {

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
  val fnUb = "src/test/resources/test_file_ub.fits"
  val fn_array = "src/test/resources/test_file_array.fits"

  // Test if the user provides the HDU index to be read
  test("HDU test: Is there a HDU number?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
    val exception = intercept[NoSuchElementException] {
      results.load(fn)
    }
    assert(exception.getMessage.contains("HDU"))
  }

  // Test if the user provides the HDU index to be read
  test("HDU test: Is HDU index above the max HDU index?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
    val exception = intercept[AssertionError] {
      results.option("hdu", 30).load(fn)
    }
    assert(exception.getMessage.contains("HDU"))
  }

  test("HDU type test: Return an empty DataFrame if HDU is empty?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits").option("hdu", 0).load(fn)
    assert(results.collect().size == 0)
  }

  test("HDU type test: Return the proper record count if HDU is an image?") {
    val fn_image = "src/test/resources/toTest/tst0009.fits"
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 2)
      .load(fn_image)
    val count = results.count()
    assert(count == 155)
  }

  // Test if the user provides the data type in the HDU
  test("HDU type test: Return an empty DF if the HDU is a Table? (not implemented yet)") {
    val fn_table = "src/test/resources/toTest/tst0009.fits"
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn_table)
    val count = results.count()
    assert(count == 0)
  }

  // Test if one accesses column as expected for HDU 1
  test("Count test: Do you count all elements in a column in HDU 1?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn)
    assert(results.select("RA").count() == 20000)
  }

  // Test if one accesses column as expected for HDU 1
  test("Count test: Do you count all elements in a column in HDU 2?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 2)
      .load(fn)
    assert(results.select("Index").count() == 20000)
  }

  // Test if one accesses column as expected for HDU 1
  test("Column test: Can you select only one column?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .option("columns", "target")
      .load(fn)
    assert(results.first.size == 1)
  }

  // Test if one accesses column as expected for HDU 1
  test("Column test: Can you select only some columns?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .option("columns", "target,RA")
      .load(fn)
    assert(results.first.size == 2)
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a Boolean?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 2)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("Discovery").first()(0).isInstanceOf[Boolean])
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a Long?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("Index").first()(0).isInstanceOf[Long])
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a Int?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("RunId").first()(0).isInstanceOf[Int])
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a Float?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("RA").first()(0).isInstanceOf[Float])
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a Double?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("Dec").first()(0).isInstanceOf[Double])
  }

  // Test if type cast is done correctly
  test("Type test: Do you see an Array(Long)?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn_array)
    // Elements of a column are arrays of 1 element
    assert(results.select("Index").schema(0).dataType.simpleString == "array<bigint>")
  }

  // Test if type cast is done correctly
  test("Type test: Do you see an Array(Float)?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn_array)
    // Elements of a column are arrays of 1 element
    assert(results.select("RA").schema(0).dataType.simpleString == "array<float>")
  }

  // Test if type cast is done correctly
  test("Type test: Do you see an Array(Double)?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn_array)
    // Elements of a column are arrays of 1 element
    assert(results.select("Dec").schema(0).dataType.simpleString == "array<double>")
  }

  // Test if type cast is done correctly
  test("Type test: Do you see an Array(Int)?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 2)
      .load(fn_array)
    // Elements of a column are arrays of 1 element
    assert(results.select("Index").schema(0).dataType.simpleString == "array<int>")
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a String?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("target").first()(0).isInstanceOf[String])
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a Byte?") {
    val results = spark.read.format("com.astrolabsoftware.sparkfits")
      .option("hdu", 1)
      .load(fnUb)
    // Elements of a column are arrays of 1 element
    assert(results.select("unsigned bytes").first()(0).isInstanceOf[Byte])
  }

}
