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
import com.sparkfits.FitsBintableLib._
import com.sparkfits.FitsImageLib._

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
    val s = fB1.blockBoundaries
    assert(fB1.empty_hdu && s.headerStart == 0 && s.dataStart == 2880 && s.dataStop == 2880 && s.blockStop == 2880)
  }

  // Check that the HDU asked is below the max HDU index.
  test("FitsLib test: Can you compute correctly the boundaries of a HDU?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val s = fB1.blockBoundaries
    assert(s.headerStart == 2880 && s.dataStart == 5760 && s.dataStop == 685760 && s.blockStop == 688320)
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
  test("FitsLib test: Can you read a short header?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val header = fB1.blockHeader
    assert(header.size <= 36)
  }

  // Check the header
  test("FitsLib test: Can you read a long header (> 2880 bytes)?") {
    val lFile = new Path("src/test/resources/test_longheader_file.fits")
    val fB1 = new FitsBlock(lFile, conf, 1)
    val header = fB1.blockHeader
    assert(header.size == 89)
  }

  // Check the header
  test("FitsLib test: The header stops by END?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val header = fB1.blockHeader
    assert(header.reverse(0).contains("END"))
  }

  // Check the reader
  test("FitsLib test: Can you read a line of the data block?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header and set the cursor at the beginning of the data block
    val header = fB1.blockHeader
    val keyValues = FitsLib.parseHeader(header)

    // Define a row and read data from the file
    val bufferSize = fB1.infos.getSizeRowBytes(keyValues).toInt
    val buffer = new Array[Byte](bufferSize)
    fB1.resetCursorAtData
    fB1.data.readFully(buffer, 0, bufferSize)

    // Convert from binary to primitive
    val row = fB1.readLineFromBuffer(buffer)

    assert(row(0) == "NGC0000000")
  }

  // Check the reader (element-by-element)
  test("FitsLib test: Can you read different element types?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header and set the cursor at the beginning of the data block
    val header = fB1.blockHeader
    val keyValues = FitsLib.parseHeader(header)
    fB1.resetCursorAtData

    val splitLocations = fB1.infos.asInstanceOf[BintableInfos].splitLocations
    val ncols = splitLocations.size - 1

    var bufferSize: Int = 0
    var buffer: Array[Byte] = null
    var el: Any = null

    for (pos <- 0 to ncols - 1) {
      // Define an element and read data from the file
      val col = pos + 1
      bufferSize = splitLocations(pos + 1) - splitLocations(pos)
      buffer = new Array[Byte](bufferSize)
      fB1.data.readFully(buffer, 0, bufferSize)

      // Convert from binary to primitive
      // shortStringValue(keyValues("TFORM" + (col + 1).toString))
      el = fB1.infos.getElementFromBuffer(buffer, shortStringValue(keyValues("TFORM" + (pos + 1).toString)))
    }

    // The next one should be the beginning of the next row
    bufferSize = splitLocations(1) - splitLocations(0)
    buffer = new Array[Byte](bufferSize)
    fB1.data.readFully(buffer, 0, bufferSize)

    // Convert from binary to primitive
    el = fB1.infos.getElementFromBuffer(buffer, shortStringValue(keyValues("TFORM" + (1).toString)))

    assert(el == "NGC0000001")
  }

  val fB1 = new FitsBlock(file, conf, 1)
  val header = fB1.blockHeader
  val keyValue = FitsLib.parseHeader(header)
  val coltypes = fB1.infos.getColTypes(keyValue)
  val s = FitsLib.shortStringValue(coltypes(0))
  test(s"shortStringValue> coltypes(0)=${coltypes(0)} => $s"){}

  // Check the column type conversion
  test("FitsLib test: Can you guess the column types?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header
    val header = fB1.blockHeader
    val keyValue = FitsLib.parseHeader(header)

    // Grab the column type (FITS standard)
    val coltypes = fB1.infos.getColTypes(keyValue)

    assert(coltypes(0) == "10A" && coltypes(1) == "E" && coltypes(2) == "D" &&
      coltypes(3) == "K" && coltypes(4) == "J")

  }

  test("FitsLib test: what is the recordLength for an image ") {
    // val file = "src/test/resources/toTest/tst0009.fits"
    // val hdu = 2
    val file = "hdfs://134.158.75.222:8020//lsst/images/a.fits"
    val hdu = 1
    val fB1 = new FitsBlock(new Path(file), conf, hdu)
    val header = fB1.blockHeader
    val keyValue = FitsLib.parseHeader(header)
    val rowSize = fB1.infos.getSizeRowBytes(keyValue)
    val nrows = fB1.infos.getNRows(keyValue)

    val startstop = fB1.getBlockBoundaries

    assert(null == s"rowSize=$rowSize nrows=$nrows startstop=${startstop}")
  }

  // Check the header keywords conversion
  test("FitsLib test: Can you grab the keywords of the header?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header
    val header = fB1.blockHeader

    // Grab the keywords
    val keys = fB1.getHeaderKeywords(header)

    assert(keys(0).contains("XTENSION"))
  }

  // Check the value conversion
  test("FitsLib test: Can you grab the values of the header?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header
    val header = fB1.blockHeader

    // Grab the values as map(keywords/values)
    val keyValues = FitsLib.parseHeader(header)

    // Check an entry with a value (BITPIX), and one without.
    // By default, header line without value gets a default value of 0.
    assert(keyValues("BITPIX").toInt == 8)
  }

  // Check the name conversion
  test("FitsLib test: Can you grab the names of the header?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header
    val header = fB1.blockHeader

    // Grab the names as map(keywords/names)
    val keyValues = FitsLib.parseHeader(header)

    val v = keyValues.
      filter(x => x._2.contains("'"))

    // Check an entry with a name (TTYPE1), and one without.
    // By default, header line without name are not taken.
    assert(FitsLib.shortStringValue(keyValues("TTYPE1")) == "target" && !v.contains("NAXIS1"))
  }

  // Check the comment conversion
  test("FitsLib test: Can you grab the comments of the header?") {
    val fB1 = new FitsBlock(file, conf, 1)

    // Read the header
    val header = fB1.blockHeader

    // Grab the names as map(keywords/names)
    val comments = fB1.getHeaderComments(header)

    // Check an entry with a comment (XTENSION).
    assert(comments("XTENSION") == "binary table extension")
  }

  // Check the reader for the number of rows
  test("FitsLib test: Can you read the number of rows?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val fB2 = new FitsBlock(file, conf, 2)

    // Read the header
    val header1 = fB1.blockHeader
    val header2 = fB2.blockHeader

    // Grab the number of rows
    val nrows1 = fB1.infos.getNRows(FitsLib.parseHeader(header1))
    val nrows2 = fB2.infos.getNRows(FitsLib.parseHeader(header2))

    assert(nrows1 == 20000 && nrows2 == 20000)
  }

  // Check the reader for the number of columns
  test("FitsLib test: Can you read the number of columns?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val fB2 = new FitsBlock(file, conf, 2)

    // Read the header
    val header1 = fB1.blockHeader
    val header2 = fB2.blockHeader

    // Grab the number of rows
    val ncols1 = fB1.infos.getNCols(FitsLib.parseHeader(header1))
    val ncols2 = fB2.infos.getNCols(FitsLib.parseHeader(header2))

    assert(ncols1 == 5 && ncols2 == 3)
  }

  // Check the reader for the size of a row
  test("FitsLib test: Can you read the size (byte) of a row?") {
    val fB1 = new FitsBlock(file, conf, 1)
    val fB2 = new FitsBlock(file, conf, 2)

    // Read the header
    val header1 = fB1.blockHeader
    val header2 = fB2.blockHeader

    // Grab the number of rows
    val rowSize1 = fB1.infos.getSizeRowBytes(FitsLib.parseHeader(header1))
    val rowSize2 = fB2.infos.getSizeRowBytes(FitsLib.parseHeader(header2))

    assert(rowSize1 == 34 && rowSize2 == 25)
  }


}
