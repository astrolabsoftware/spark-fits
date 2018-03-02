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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.types._

import com.sparkfits.FitsLib._
import com.sparkfits.FitsSchema._

/**
  * Test class for the FitsSchema object.
  */
class FitsLibTest extends FunSuite with BeforeAndAfterAll {

  // Open the test fits file and get meta info
  val file = new Path("src/test/resources/test_file.fits")
  val conf = new Configuration()

  // Easy
  test("FitsLib test: Can you initialise a FITS HDU?") {
    val fB1 = new FitsBlock(file, conf, 1)
    assert(fB1.isInstanceOf[FitsBlock])
  }

  // Check that the HDU asked is below the max HDU index.
  test("FitsLib test: Can you detect wrong HDU index?") {
    val exception = intercept[AssertionError] {
      new FitsBlock(file, conf, 16)
    }
    assert(exception.getMessage.contains("HDU number"))
  }

  // Check that the HDU asked is below the max HDU index.
  test("FitsLib test: Can you initialise correctly an empty HDU?") {
    val fB1 = new FitsBlock(file, conf, 0)
    val s = fB1.BlockBoundaries
    assert(fB1.empty_hdu && s._1 == 0 && s._2 == 2880 && s._3 == 2880 && s._4 == 2880)
  }

  // Check that the HDU asked is below the max HDU index.
  test("FitsLib test: Can you compute correctly the boundaries of a HDU?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val s = fB1.BlockBoundaries
    assert(s._1 == 2880 && s._2 == 5760 && s._3 == 685760 && s._4 == 688320)
  }

  // Check the total number of HDU
  test("FitsLib test: Can you get the total number of HDU?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val n = fB1.getNHDU
    assert(n == 3)
  }

  // Check custom cursors
  test("FitsLib test: Can you play with the cursor (header)?") {
    val fB1 = new FitsBlock(file, conf, 1)
    fB1.resetCursorAtHeader
    assert(fB1.data.getPos == 2880)
  }

  // Check custom cursors
  test("FitsLib test: Can you play with the cursor (data)?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val n = fB1.resetCursorAtData
    assert(fB1.data.getPos == 5760)
  }

  // Check custom cursors
  test("FitsLib test: Can you play with the cursor (general)?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val n = fB1.setCursor(589)
    assert(fB1.data.getPos == 589)
  }

  // Check the header
  test("FitsLib test: Can you read the header?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val header = fB1.readHeader
    assert(header.size <= 36)
  }

  // Check the header
  test("FitsLib test: The header stops by END?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val header = fB1.readHeader
    assert(header.reverse(0).contains("END"))
  }

  // Check the reader
  test("FitsLib test: Can you read a line of the data block?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header and set the cursor at the beginning of the data block
    val header = fB1.readHeader

    // Define a row and read data from the file
    val bufferSize = fB1.getSizeRowBytes(header).toInt
    val buffer = new Array[Byte](bufferSize)
    fB1.data.readFully(buffer, 0, bufferSize)

    // Convert from binary to primitive
    val row = fB1.readLineFromBuffer(buffer)

    assert(row(0) == "NGC0000000")
  }

  // Check the reader (element-by-element)
  test("FitsLib test: Can you read different element types?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header and set the cursor at the beginning of the data block
    val header = fB1.readHeader

    val splitLocations = fB1.splitLocations
    val ncols = splitLocations.size - 1

    var bufferSize: Int = 0
    var buffer: Array[Byte] = null
    var el: Any = null

    for (pos <- 0 to ncols - 1) {
      // Define an element and read data from the file
      bufferSize = splitLocations(pos+1) - splitLocations(pos)
      buffer = new Array[Byte](bufferSize)
      fB1.data.readFully(buffer, 0, bufferSize)

      // Convert from binary to primitive
      el = fB1.getElementFromBuffer(buffer, fB1.rowTypes(pos))
    }

    // The next one should be the beginning of the next row
    bufferSize = splitLocations(1) - splitLocations(0)
    buffer = new Array[Byte](bufferSize)
    fB1.data.readFully(buffer, 0, bufferSize)

    // Convert from binary to primitive
    el = fB1.getElementFromBuffer(buffer, fB1.rowTypes(0))

    assert(el == "NGC0000001")
  }

  // Check the column type conversion
  test("FitsLib test: Can you guess the column types?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header
    val header = fB1.readHeader

    // Grab the column type (FITS standard)
    val coltypes = fB1.getColTypes(header)

    assert(coltypes(0) == "10A" && coltypes(1) == "E" && coltypes(2) == "D" &&
      coltypes(3) == "K" && coltypes(4) == "J")

  }

  // Check the header keywords conversion
  test("FitsLib test: Can you grab the keywords of the header?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header
    val header = fB1.readHeader

    // Grab the keywords
    val keys = fB1.getHeaderKeywords(header)

    assert(keys(0).contains("XTENSION"))
  }

  // Check the value conversion
  test("FitsLib test: Can you grab the values of the header?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header
    val header = fB1.readHeader

    // Grab the values as map(keywords/values)
    val values = fB1.getHeaderValues(header)

    // Check an entry with a value (BITPIX), and one without.
    // By default, header line without value gets a default value of 0.
    assert(values("BITPIX") == 8 && values("TTYPE1") == 0)
  }

  // Check the name conversion
  test("FitsLib test: Can you grab the names of the header?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header
    val header = fB1.readHeader

    // Grab the names as map(keywords/names)
    val names = fB1.getHeaderNames(header)

    // Check an entry with a name (TTYPE1), and one without.
    // By default, header line without name gets a default value of "".
    assert(names("TTYPE1") == "target" && names("NAXIS1") == "")
  }

  // Check the comment conversion
  test("FitsLib test: Can you grab the comments of the header?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header
    val header = fB1.readHeader

    // Grab the names as map(keywords/names)
    val comments = fB1.getHeaderComments(header)

    // Check an entry with a comment (XTENSION), and one without.
    // By default, header line without comment gets a default value of "".
    assert(comments("XTENSION") == "binary table extension" &&
      comments("TTYPE1") == "")
  }

  // Check the reader for the number of rows
  test("FitsLib test: Can you read the number of rows?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val fB2 = new FitsBlock(file, conf, 2)

    // Read the header
    val header1 = fB1.readHeader
    val header2 = fB2.readHeader

    // Grab the number of rows
    val nrows1 = fB1.getNRows(header1)
    val nrows2 = fB2.getNRows(header2)

    assert(nrows1 == 20000 && nrows2 == 20000)
  }

  // Check the reader for the number of columns
  test("FitsLib test: Can you read the number of columns?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val fB2 = new FitsBlock(file, conf, 2)

    // Read the header
    val header1 = fB1.readHeader
    val header2 = fB2.readHeader

    // Grab the number of rows
    val ncols1 = fB1.getNCols(header1)
    val ncols2 = fB2.getNCols(header2)

    assert(ncols1 == 5 && ncols2 == 3)
  }

  // Check the reader for the size of a row
  test("FitsLib test: Can you read the size (byte) of a row?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val fB2 = new FitsBlock(file, conf, 2)

    // Read the header
    val header1 = fB1.readHeader
    val header2 = fB2.readHeader

    // Grab the number of rows
    val rowSize1 = fB1.getSizeRowBytes(header1)
    val rowSize2 = fB2.getSizeRowBytes(header2)

    assert(rowSize1 == 34 && rowSize2 == 25)
  }


}
