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

import org.apache.spark.sql.types._

import com.sparkfits.FitsHdu._

/**
  * Contain class and methods to manipulate Image HDU.
  */
object FitsHduImage {
  case class ImageHDU(pixelSize: Int, axis: Array[Long]) extends HDU {

    /** Image HDU are implemented */
    override def implemented: Boolean = {true}

    /**
      * Get the number of row of a HDU.
      * We rely on what's written in the header, meaning
      * here we do not access the data directly.
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @return (Long), the number of rows as written in KEYWORD=NAXIS2.
      *
      */
    override def getNRows(keyValues: Map[String, String]) : Long = {
      val totalBytes = axis.reduce(_ * _) * pixelSize
      val rowBytes = getSizeRowBytes(keyValues)

      val result = if (totalBytes % rowBytes == 0) {
        (totalBytes / rowBytes).toLong
      }
      else {
        ((totalBytes / rowBytes) + 1).toLong
      }

      result
    }

    override def getSizeRowBytes(keyValues: Map[String, String]) : Int = {
      // println(s"FitsImageLib.ImageHDU.getSizeRowBytes> ")
      var size = (pixelSize * axis(0)).toInt
      // Try and get the integer division factor until size becomes lower than 1024
      var factor = 2
      do {
        if (size % factor == 0) {
          size /= factor
        }
        else {
          factor += 1
        }
      } while (size > 1024)
      size
    }

    override def getNCols(keyValues : Map[String, String]) : Long = {
      1L
    }

    /**
      * Return the types of elements for each column as a list.
      *
      * @param col : (Int)
      *   Column index used for the recursion.
      * @return (List[String]), list with the types of elements for each column
      *   as given by the header.
      *
      */
    override def getColTypes(keyValues : Map[String, String]): List[String] = {
      // Get the names of the Columns
      val colTypes = List.newBuilder[String]
      colTypes += "Image"
      colTypes.result
    }

    override def listOfStruct : List[StructField] = {
      // Get the list of StructField.
      val lStruct = List.newBuilder[StructField]
      lStruct += StructField("Image", ArrayType(ByteType, true))
      lStruct.result
    }

    override def getRow(buf: Array[Byte]): List[Any] = {
      val row = List.newBuilder[Any]
      row += buf
      row.result
    }

    override def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {
      subbuf
    }
  }
}
