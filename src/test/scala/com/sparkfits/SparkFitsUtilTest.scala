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
