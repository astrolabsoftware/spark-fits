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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.types.StructField

import com.astrolabsoftware.sparkfits.FitsLib.shortStringValue

object FitsHdu {

  /**
    * Trait containing generic informations concerning HDU informations.
    * This includes for example number of rows, size of a row,
    * number of columns, types of elements, and methods to access elements.
    *
    */
  trait HDU {

    // 8 bits in one byte
    val BYTE_SIZE = 8

    /**
      * Check whether the HDU is implemented in the library.
      * Must be set for all extensions of HDU trait.
      *
      * @return (Boolean)
      */
    def implemented: Boolean

    /**
      * Generic method to return the number of rows from the header information.
      * To be implemented in specific HDU.
      *
      * @param keyValues : (Map[String, String])
      *   (Key, Values) from the header (see parseHeader)
      * @return (Long) Number of rows in the data HDU.
      *
      */
    def getNRows(keyValues: Map[String, String]) : Long

    /**
      * Generic method to get the size of one row (bytes) from the header
      * information.
      * Must be implemented for all HDU extensions.
      *
      * @param keyValues : (Map[String, String])
      *   (Key, Values) from the header (see parseHeader)
      * @return (Long) Size in bytes of one row.
      *
      */
    def getSizeRowBytes(keyValues: Map[String, String]) : Int

    /**
      * Generic method to get the number of columns from the header information.
      * Must be implemented for all extensions of HDU.
      *
      * @param keyValues : (Map[String, String])
      *   (Key, Values) from the header (see parseHeader)
      * @return (Long) Number of columns in the data HDU.
      *
      */
    def getNCols(keyValues : Map[String, String]) : Long

    /**
      * Generic method to get the types of column elements from the header
      * information.
      * Must be implemented for all extensions of HDU.
      *
      * @param keyValues : (Map[String, String])
      *   (Key, Values) from the header (see parseHeader)
      * @return (List[String]) Types of elements of columns.
      *
      */
    def getColTypes(keyValues : Map[String, String]): List[String]

    /**
      * Generic method to convert header information into StructField used to
      * build the DataFrame schema.
      * Must be implemented for all extensions of HDU.
      *
      * @return (List[StructField]) List of StructField containing name and
      *   types of columns.
      */
    def listOfStruct : List[StructField]

    /**
      * Generic method to decode the rows of the data block.
      * Must be implemented for all extensions of HDU.
      *
      * @param buf : (Array[Bytes])
      *   Array of Bytes describing one row.
      * @return (List[Any]) The decoded row containing primitives.
      *
      */
    def getRow(buf: Array[Byte]): List[Any]

    /**
      * Convert one array of bytes corresponding to one element of
      * the table into its primitive type.
      *
      * @param subbuf : (Array[Byte])
      *   Array of byte describing one element of the table.
      * @param fitstype : (String)
      *   The type of this table element according to the header.
      * @return the table element converted from binary.
      *
      */
    def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {
      // Grab the type of the element
      val shortType = shortStringValue{fitstype}

      shortType match {
        // 16-bit Integer
        case x if shortType.contains("I") => {
          ByteBuffer.wrap(subbuf, 0, 2).getShort()
        }
        // 32-bit Integer
        case x if shortType.contains("J") => {
          ByteBuffer.wrap(subbuf, 0, 4).getInt()
        }
        // 64-bit Integer
        case x if shortType.contains("K") => {
          ByteBuffer.wrap(subbuf, 0, 8).getLong()
        }
        // Single precision floating-point
        case x if shortType.contains("E") => {
          ByteBuffer.wrap(subbuf, 0, 4).getFloat()
        }
        // Double precision floating-point
        case x if shortType.contains("D") => {
          ByteBuffer.wrap(subbuf, 0, 8).getDouble()
        }
        // Boolean
        case x if shortType.contains("L") => {
          // 1 Byte containing the ASCII char T(rue) or F(alse).
          subbuf(0).toChar == 'T'
        }
        // ISSUE-59: 8-bit (1 byte) unsigned byte
        case x if shortType.contains("B") => {
          ByteBuffer.wrap(subbuf, 0, 1).get()
        }
        // Number of bits
        case x if shortType.endsWith("X") => {
          List(subbuf)
        }
        // Chain of characters
        case x if shortType.endsWith("A") => {
          // Example 20A means string on 20 bytes
          new String(subbuf, StandardCharsets.UTF_8).trim()
        }
        case _ => {
          println(s"""
            FitsHdu.getElementFromBuffer> Cannot infer size of type
            $shortType from the header! See getElementFromBuffer
              """)
          0
        }
      }
    }
  }

  /**
    * Generic class extending Infos concerning dummy HDU (e.g. not implemented).
    * Set all variables and methods to null/0/false.
    */
  case class AnyHDU() extends HDU {

    /** Empty HDU not implemented. */
    override def implemented: Boolean = {false}

    /** Return no row */
    override def getNRows(keyValues: Map[String, String]) : Long = {0L}

    /** Rows have size zero */
    override def getSizeRowBytes(keyValues: Map[String, String]) : Int = {0}

    /** Return no columns */
    override def getNCols(keyValues : Map[String, String]) : Long = {0L}

    /** Elements have no types */
    override def getColTypes(keyValues : Map[String, String]): List[String] = {null}

    /** Return no schema structure */
    override def listOfStruct : List[StructField] = {null}

    /** Return null row */
    override def getRow(buf: Array[Byte]): List[Any] = {null}

    /** Return null element */
    override def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {null}
  }
}
