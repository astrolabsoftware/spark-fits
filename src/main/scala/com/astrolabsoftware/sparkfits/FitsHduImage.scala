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

import org.apache.spark.sql.types._

import com.astrolabsoftware.sparkfits.FitsHdu._
import com.astrolabsoftware.sparkfits.FitsSchema.ReadMyType

/**
  * Contain class and methods to manipulate Image HDU.
  */
object FitsHduImage {
  case class ImageHDU(header : Array[String]) extends HDU {

    // Initialise the key/value from the header.
    val keyValues = FitsLib.parseHeader(header)

    // Compute the dimension of the image
    // BITPIX is positive for short/int/long and negative for float/double
    val elementSize = math.abs((keyValues("BITPIX").toInt) / BYTE_SIZE)
    val dimensions = keyValues("NAXIS").toInt

    // NAXIS1  = x dimension
    // NAXIS2  = y dimension
    // NAXIS3  = z dimension
    // NAXIS...  = ... dimension
    val axisBuilder = Array.newBuilder[Long]
    for (d <- 1 to dimensions){
      axisBuilder += keyValues("NAXIS" + d.toString).toLong
    }
    val axis = axisBuilder.result

    // Initialise type and byte size of image elements.
    val elementType = getColTypes(keyValues)

    /** Image HDU are implemented */
    override def implemented: Boolean = {true}

    /**
      * Get the number of row of a image HDU, that is the product of NAXISn
      * for n>1. We rely on what's written in the header, meaning
      * here we do not access the data directly.
      *
      * @param keyValues : (Map[String, String])
      *   Key/Value from the header of the HDU.
      * @return (Long), the number of rows as PI_{n>1}(NAXISn)
      *
      */
    override def getNRows(keyValues: Map[String, String]) : Long = {
      axis.reduce(_ * _) / axis(0)
    }

    /**
      * Return the size in bytes of one image row.
      * This is given by NAXIS1 in the FITS header.
      *
      * @param keyValues : (Map[String, String])
      *  Key/Values from the Fits header (see parseHeader)
      * @return (Int) the size in bytes of one image row.
      *
      */
    override def getSizeRowBytes(keyValues: Map[String, String]) : Int = {
      axis(0).toInt * elementSize
    }

    /**
      * Number of columns for image is set by default to one.
      *
      * @return (Long) : 1L
      */
    override def getNCols(keyValues : Map[String, String]) : Long = {
      // Could be the number of images in the z dimension?
      // axis(2)
      1L
    }

    /**
      * Return the type of image elements.
      * BITPIX is positive for short/int/long and negative for float/double.
      *
      * @param keyValues : (Map[String, String])
      *   Key/Value pairs from the Fits header (see parseHeader)
      * @return (List[String]), list of one element containing the type.
      *
      */
    override def getColTypes(keyValues : Map[String, String]): List[String] = {

      // BITPIX is positive for short/int/long and negative for float/double
      val bitpix = keyValues("BITPIX").toInt / BYTE_SIZE
      bitpix match {
        case 1 => List("L")
        case 2 => List("I")
        case 4 => List("J")
        case 8 => List("K")
        case -4 => List("E")
        case -8 => List("D")
        case _ => println(s"""
          FitsHduImage.getColTypes> Cannot infer size of data
          from the header!
            """)
        List("")
      }
    }

    /**
      *
      * Build a list of one StructField from header information.
      * The list of StructField is then used to build the DataFrame schema.
      *
      * @return (List[StructField]) List of StructField with column names [Image, ImgIndex],
      *   data type, and whether the data is nullable.
      *
      */
    override def listOfStruct : List[StructField] = {
      // Get the list of StructField.
      val lStruct = List.newBuilder[StructField]
      val img = ReadMyType("Image", elementType(0), true)
      val index = ReadMyType("ImgIndex", "K", true)
      lStruct += img.copy(img.name, ArrayType(img.dataType))
      lStruct += index.copy(index.name, index.dataType)
      lStruct.result
    }

    /**
      * Convert an image row from binary to primitives.
      *
      * @param buf : (Array[Byte])
      *   Array of bytes.
      * @return (List[List[Any]]) : Decoded row as a list of one list of primitives.
      *
      */
    override def getRow(buf: Array[Byte]): List[List[Any]] = {
      // Number of primitives to decode
      val nelements_per_row = buf.size / elementSize

      // Loop over elements
      val row = List.newBuilder[Any]
      for (pos <- 0 to nelements_per_row - 1) {
        row += getElementFromBuffer(
          buf.slice(pos * elementSize, (pos+1)*elementSize), elementType(0))
      }

      // Return a List of List
      List(row.result)
    }
  }
}
