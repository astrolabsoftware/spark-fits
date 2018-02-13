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

  test("Schema test: can you convert the type for a column?") {
    val col = ReadMyType("RA", hdu.getColumnFormat(1))

    assert(col.dataType.isInstanceOf[FloatType])
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
