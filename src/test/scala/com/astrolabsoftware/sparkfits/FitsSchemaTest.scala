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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.types._

import com.astrolabsoftware.sparkfits.FitsLib._
import com.astrolabsoftware.sparkfits.FitsSchema._

/**
  * Test class for the FitsSchema object.
  */
class FitsSchemaTest extends FunSuite with BeforeAndAfterAll {

  // Open the test fits file and get meta info
  val file = new Path("src/test/resources/test_file.fits")
  val conf = new Configuration()
  val fB1 = new Fits(file, conf, 1)
  val header1 = fB1.blockHeader
  val fB2 = new Fits(file, conf, 2)
  val header2 = fB2.blockHeader

  test("Schema test: can you convert the type Float for a column?") {
    val col = ReadMyType("toto", "E")

    assert(col.dataType.isInstanceOf[FloatType])
  }

  test("Schema test: can you convert the type Array(Float) for a column?") {
    val col = ReadMyType("toto", "1E")

    assert(col.dataType.isInstanceOf[ArrayType])
  }

  test("Schema test: can you convert the type Double for a column?") {
    val col = ReadMyType("toto", "D")

    assert(col.dataType.isInstanceOf[DoubleType])
  }

  test("Schema test: can you convert the type Array(Double) for a column?") {
    val col = ReadMyType("toto", "2D")

    assert(col.dataType.isInstanceOf[ArrayType])
  }

  test("Schema test: can you convert the type String for a column?") {
    val col = ReadMyType("toto", "28A")

    assert(col.dataType.isInstanceOf[StringType])
  }

  test("Schema test: can you convert the type Short for a column?") {
    val col = ReadMyType("toto", "I")

    assert(col.dataType.isInstanceOf[ShortType])
  }

  test("Schema test: can you convert the type Array(Short) for a column?") {
    val col = ReadMyType("toto", "3I")

    assert(col.dataType.isInstanceOf[ArrayType])
  }

  test("Schema test: can you convert the type Int for a column?") {
    val col = ReadMyType("toto", "J")

    assert(col.dataType.isInstanceOf[IntegerType])
  }

  test("Schema test: can you convert the type Array(Int) for a column?") {
    val col = ReadMyType("toto", "4J")

    assert(col.dataType.isInstanceOf[ArrayType])
  }

  test("Schema test: can you convert the type Long for a column?") {
    val col = ReadMyType("toto", "K")

    assert(col.dataType.isInstanceOf[LongType])
  }

  test("Schema test: can you convert the type Array(Long) for a column?") {
    val col = ReadMyType("toto", "5K")

    assert(col.dataType.isInstanceOf[ArrayType])
  }

  test("Schema test: can you convert the type Boolean for a column?") {
    val col = ReadMyType("toto", "L")

    assert(col.dataType.isInstanceOf[BooleanType])
  }

  test("Schema test: can you convert the type Unsigned Byte for a column?") {
    val col = ReadMyType("toto", "B")

    assert(col.dataType.isInstanceOf[ByteType])
  }

  test("Schema test: can you convert the name for a column?") {
    val col = ReadMyType("toto", "E")

    assert(col.name == "toto")
  }

  // val keyValues = FitsLib.parseHeader(header1)
  // test(s"getNCols> keyValues=${keyValues.toString} tfields=${keyValues("TFIELDS")}"){}

  test("Schema test: can you generate a list for all columns?") {
    val ncols = fB1.hdu.getNCols(FitsLib.parseHeader(header1))
    val myList = ListOfStruct(fB1)

    assert(myList.size == ncols)
  }

  test("Schema test: can you generate a schema from the hdu header?") {
    val schema1 = getSchema(fB1)
    val schema2 = getSchema(fB2)

    assert(schema1.isInstanceOf[StructType] && schema2.isInstanceOf[StructType])
  }
}
