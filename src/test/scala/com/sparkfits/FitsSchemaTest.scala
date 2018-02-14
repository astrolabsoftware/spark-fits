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

import org.apache.spark.sql.types._

import nom.tam.fits.BinaryTableHDU
import nom.tam.fits.Fits

import com.sparkfits.FitsSchema._

/**
  * Test class for the FitsSchema object.
  */
class FitsSchemaTest extends FunSuite with BeforeAndAfterAll {

  // Open the test fits file and get meta info
  val fn = "src/test/resources/test.fits"
  val f = new Fits(fn)
  val hdu = f.getHDU(1).asInstanceOf[BinaryTableHDU]
  val hdu_2 = f.getHDU(2).asInstanceOf[BinaryTableHDU]

  test("Schema test: can you convert the type Float for a column?") {
    val col = ReadMyType("RA", hdu.getColumnFormat(1))

    assert(col.dataType.isInstanceOf[FloatType])
  }

  test("Schema test: can you convert the type String for a column?") {
    val col = ReadMyType("target", hdu_2.getColumnFormat(0))

    assert(col.dataType.isInstanceOf[StringType])
  }

  test("Schema test: can you convert the type Int for a column?") {
    val col = ReadMyType("index", hdu_2.getColumnFormat(1))

    assert(col.dataType.isInstanceOf[IntegerType])
  }

  test("Schema test: can you convert the type Boolean for a column?") {
    val col = ReadMyType("Discovery", hdu_2.getColumnFormat(2))

    assert(col.dataType.isInstanceOf[BooleanType])
  }

  test("Schema test: can you convert the name for a column?") {
    val col = ReadMyType(hdu.getColumnName(1), "E")

    assert(col.name == "RA")
  }

  test("Schema test: can you generate a list for all columns?") {
    val ncols = hdu.getNCols
    val myList = ListOfStruct(hdu, 0, ncols)

    assert(myList.size == ncols)
  }

  test("Schema test: can you generate a schema from the hdu header?") {
    val schema = getSchema(hdu)

    assert(schema.isInstanceOf[StructType])
  }
}
