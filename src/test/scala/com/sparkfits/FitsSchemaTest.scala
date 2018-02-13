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
