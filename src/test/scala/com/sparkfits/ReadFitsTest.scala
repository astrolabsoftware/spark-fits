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

import org.apache.log4j.Level
import org.apache.log4j.Logger

import com.sparkfits.fits._

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

  // // Test if the user provides the data type in the HDU
  // test("dataType test: is there table or image in options?") {
  //   val results = spark.readfits
  //   val exception = intercept[NoSuchElementException] {
  //     results.load(fn)
  //   }
  //   assert(exception.getMessage.contains("datatype"))
  // }
  //
  // // Test if the data type is table (image not yet supported)
  // test("dataType test: Is the datatype a table?") {
  //   val results = spark.readfits
  //   val exception = intercept[AssertionError] {
  //     results.option("datatype", "image").load(fn)
  //   }
  //   assert(exception.getMessage.contains("datatype"))
  // }

  // Test if the user provides the HDU index to be read
  test("HDU test: Is there a HDU number?") {
    val results = spark.readfits
    val exception = intercept[NoSuchElementException] {
      results.option("datatype", "table").load(fn)
    }
    assert(exception.getMessage.contains("HDU"))
  }

  // Test if the user provides the HDU index to be read
  test("HDU test: Is HDU index above the max HDU index?") {
    val results = spark.readfits
    val exception = intercept[AssertionError] {
      results.option("datatype", "table").option("HDU", 30).load(fn)
    }
    assert(exception.getMessage.contains("HDU"))
  }

  // Test if the user provides the data type in the HDU
  test("Table test: Are you really accessing a Table?") {
    val results = spark.readfits
    val exception = intercept[AssertionError] {
      results.option("datatype", "table").option("HDU", 0).load(fn)
    }
    assert(exception.getMessage.contains("BINTABLE"))
  }

  // Test if one accesses column as expected for HDU 1
  test("Count test: Do you count all elements in a column in HDU 1?") {
    val results = spark.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .load(fn)
    assert(results.select("RA").count() == 20000)
  }

  // Test if one accesses column as expected for HDU 1
  test("Count test: Do you count all elements in a column in HDU 2?") {
    val results = spark.readfits
      .option("datatype", "table")
      .option("HDU", 2)
      .load(fn)
    assert(results.select("Index").count() == 20000)
  }

  // Test if one accesses column as expected for HDU 1
  test("Column test: Can you select only some columns?") {
    val results = spark.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .option("columns", "target")
      .load(fn)
    assert(results.first.size == 1)
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a Boolean?") {
    val results = spark.readfits
      .option("datatype", "table")
      .option("HDU", 2)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("Discovery").first()(0).isInstanceOf[Boolean])
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a Long?") {
    val results = spark.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("Index").first()(0).isInstanceOf[Long])
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a Int?") {
    val results = spark.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("RunId").first()(0).isInstanceOf[Int])
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a Float?") {
    val results = spark.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("RA").first()(0).isInstanceOf[Float])
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a Double?") {
    val results = spark.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("Dec").first()(0).isInstanceOf[Double])
  }

  // Test if type cast is done correctly
  test("Type test: Do you see a String?") {
    val results = spark.readfits
      .option("datatype", "table")
      .option("HDU", 1)
      .load(fn)
    // Elements of a column are arrays of 1 element
    assert(results.select("target").first()(0).isInstanceOf[String])
  }

}
