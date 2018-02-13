package com.sparkfits

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import nom.tam.fits.BinaryTableHDU
import nom.tam.fits.Fits

import com.sparkfits.SparkFitsUtil._

/**
  * Test class for the FitsSchema object.
  */
class SparkFitsUtilTest extends FunSuite with BeforeAndAfterAll {

  // Open the test fits file and get meta info
  val fn = "src/test/resources/test.fits"
  val f = new Fits(fn)
  val hdu = f.getHDU(1).asInstanceOf[BinaryTableHDU]

  test("SparkFitsUtil test: Can you guess the number of HDU?") {
    val nHDU = getNHdus(f)

    assert(nHDU == 3)
  }

  test("SparkFitsUtil test: Can you grab all elements in the header?") {
    val it = hdu.getHeader.iterator
    val myheader = getMyHeader(it, "")
    val nElements = myheader.split(",").size

    assert(nElements == 17)
  }
}
