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

object FitsHdu {

  /**
    * Trait containing generic informations concerning HDU informations.
    * This includes for example number of rows, size of a row,
    * number of columns, types of elements, and methods to access elements.
    *
    */
  trait HDU {

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
      * Generic method to decode one row element of the data block.
      * Must be implemented for all extensions of HDU.
      *
      * @param subbuf : (Array[Bytes])
      *   Array of Bytes describing one element.
      * @return (Any) The element decoded.
      *
      */
    def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any

  }

  /**
    * Generic class extending Infos concerning dummy HDU (e.g. not implemented).
    * Set all variables and methods to null/0/false.
    */
  case class AnyHDU() extends HDU {

    /** Empty HDU not implemented. */
    def implemented: Boolean = {false}

    /** Return no row */
    def getNRows(keyValues: Map[String, String]) : Long = {0L}

    /** Rows have size zero */
    def getSizeRowBytes(keyValues: Map[String, String]) : Int = {0}

    /** Return no columns */
    def getNCols(keyValues : Map[String, String]) : Long = {0L}

    /** Elements have no types */
    def getColTypes(keyValues : Map[String, String]): List[String] = {null}

    /** Return no schema structure */
    def listOfStruct : List[StructField] = {null}

    /** Return null row */
    def getRow(buf: Array[Byte]): List[Any] = {null}

    /** Return null element */
    def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {null}
  }
}
