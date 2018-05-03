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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.util.Try

import org.apache.spark.sql.types.StructField

import com.sparkfits.FitsHdu._
import com.sparkfits.FitsSchema.ReadMyType

/**
  * Contain class and methods to manipulate Bintable HDU.
  */
object FitsHduBintable {

  /**
    * Main class for Bintable HDU
    */
  case class BintableHDU(header : Array[String],
    selectedColumns: List[String] = null) extends HDU {

    val keyValues = FitsLib.parseHeader(header)

    // Check if the user specifies columns to select
    val colNames = keyValues.
      filter(x => x._1.contains("TTYPE")).
      map(x => (x._1, x._2.split("'")(1).trim()))

    val selectedColNames = if (selectedColumns != null) {
      selectedColumns
    } else {
      colNames.values.toList.asInstanceOf[List[String]]
    }

    val colPositions = selectedColNames.map(
      x => getColumnPos(keyValues, x)).toList.sorted

    val rowTypes = getColTypes(keyValues)

    val ncols = rowTypes.size

    // splitLocations is an array containing the location of elements
    // (byte index) in a row. Example if we have a row with [20A, E, E], one
    // will have splitLocations = [0, 20, 24, 28] that is a string on 20 Bytes,
    // followed by 2 floats on 4 bytes each.
    val splitLocations = (0 :: rowSplitLocations(rowTypes, 0)).scan(0)(_ +_).tail

    // // Declare useful vars for later
    // var rowTypes: List[String] = List()
    // var colNames: Map[String, String] = Map()
    // var selectedColNames: List[String] = List()
    // var colPositions: List[Int] = List()
    // var splitLocations: List[Int] = List()

    /** Bintables are implemented */
    override def implemented: Boolean = {true}

    /**
      * Get the number of row of a HDU.
      * We rely on what's written in the header, meaning
      * here we do not access the data directly.
      *
      * @param keyValues : (Map[String, String])
      *   keyValues from the header of the HDU (see parseHeader).
      * @return (Long), the number of rows as written in KEYWORD=NAXIS2.
      *
      */
    override def getNRows(keyValues : Map[String, String]) : Long = {
      keyValues("NAXIS2").toLong
    }

    /**
      * Get the size (bytes) of each row of a HDU.
      * We rely on what's written in the header, meaning
      * here we do not access the data directly.
      *
      * @param keyValues : (Map[String, String])
      *   keyValues from the header of the HDU (see parseHeader).
      * @return (Int), the size (bytes) of one row as written in KEYWORD=NAXIS1.
      *
      */
    override def getSizeRowBytes(keyValues: Map[String, String]) : Int = {
      keyValues("NAXIS1").toInt
    }

    /**
      * Get the number of column of a HDU.
      * We rely on what's written in the header, meaning
      * here we do not access the data directly.
      *
      * @param keyValues : (Map[String, String])
      *   keyValues from the header of the HDU (see parseHeader).
      * @return (Long), the number of rows as written in KEYWORD=TFIELDS.
      *
      */
    override def getNCols(keyValues : Map[String, String]) : Long = {
      if (keyValues.contains("TFIELDS")) {
        keyValues("TFIELDS").toLong
      } else 0L
    }

    /**
      * Return the types of elements for each column as a list.
      *
      * @param keyValues : (Map[String, String])
      *   keyValues from the header of the HDU (see parseHeader).
      * @return (List[String]), list with the types of elements for each column
      *   as given by the header.
      *
      */
    override def getColTypes(keyValues: Map[String, String]): List[String] = {
      val colTypes = List.newBuilder[String]

      val ncols = getNCols(keyValues).toInt

      for (col <- 0 to ncols-1) {
        colTypes += getColType(keyValues, col)
      }
      colTypes.result
    }

    /**
      * Get the type of the elements of a column with index `colIndex` of a HDU.
      *
      * @param keyValues : (Map[String, String])
      *   The header of the HDU.
      * @param colIndex : (Int)
      *   Index (zero-based) of a column.
      * @return (String), the type (FITS convention) of the elements of the column.
      *
      */
    def getColType(keyValues : Map[String, String], colIndex : Int) : String = {
      // Zero-based index
      FitsLib.shortStringValue(keyValues("TFORM" + (colIndex + 1).toString))
    }

    /**
      *
      * Build a list of StructField from header information.
      * The list of StructField is then used to build the DataFrame schema.
      *
      * @return (List[StructField]) List of StructField with column name,
      *   column type, and whether the column is nullable.
      *
      */
    override def listOfStruct : List[StructField] = {
      // Initialise the list of StructField.
      val lStruct = List.newBuilder[StructField]

      // Loop over necessary columns specified by the user.
      for (colIndex <- colPositions) {
        // Column name
        val colName = FitsLib.shortStringValue(
          colNames("TTYPE" + (colIndex + 1).toString))

        // Full structure
        lStruct += ReadMyType(colName, rowTypes(colIndex))
      }

      // Return the result
      lStruct.result
    }

    /**
      * Convert a bintable row elements from binary to primitives.
      *
      */
    override def getRow(buf: Array[Byte]): List[Any] = {
      var row = List.newBuilder[Any]

      for (col <- colPositions) {
        row += getElementFromBuffer(
          buf.slice(splitLocations(col), splitLocations(col+1)), rowTypes(col))
      }
      row.result
    }

    /**
      * Companion to readLineFromBuffer. Convert one array of bytes
      * corresponding to one element of the table into its primitive type.
      *
      * @param subbuf : (Array[Byte])
      *   Array of byte describing one element of the table.
      * @param fitstype : (String)
      *   The type of this table element according to the header.
      * @return the table element converted from binary.
      *
      */
    override def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {
      // Grab the type of the element
      val shortType = FitsLib.shortStringValue{fitstype}

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
        // Chain of characters
        case x if shortType.endsWith("A") => {
          // Example 20A means string on 20 bytes
          new String(subbuf, StandardCharsets.UTF_8).trim()
        }
        case _ => {
          println(s"""
            FitsLib.getElementFromBuffer> Cannot infer size of type
            $shortType from the header! See getElementFromBuffer
              """)
          0
        }
      }
    }

    /**
      * Description of a row in terms of bytes indices.
      * rowSplitLocations returns an array containing the position of elements
      * (byte index) in a row. Example if we have a row with [20A, E, E], one
      * will have rowSplitLocations -> [0, 20, 24, 28] that is a string
      * on 20 Bytes, followed by 2 floats on 4 bytes each.
      *
      * @param col : (Int)
      *   Column position used for the recursion. Should be left at 0.
      * @return (List[Int]), the position of elements (byte index) in a row.
      *
      */
    def rowSplitLocations(rowTypes: List[String], col : Int = 0) : List[Int] = {
      val ncols = rowTypes.size

      if (col == ncols) {
        Nil
      } else {
        getSplitLocation(rowTypes(col)) :: rowSplitLocations(rowTypes, col + 1)
      }
    }

    /**
      * Companion routine to rowSplitLocations. Returns the size of a primitive
      * according to its type from the FITS header.
      *
      * @param fitstype : (String)
      *   Element type according to FITS standards (I, J, K, E, D, L, A, etc)
      * @return (Int), the size (bytes) of the element.
      *
      */
    def getSplitLocation(fitstype : String) : Int = {
      val shortType = FitsLib.shortStringValue(fitstype)

      shortType match {
        case x if shortType.contains("I") => 2
        case x if shortType.contains("J") => 4
        case x if shortType.contains("K") => 8
        case x if shortType.contains("E") => 4
        case x if shortType.contains("D") => 8
        case x if shortType.contains("L") => 1
        case x if shortType.endsWith("A") => {
          // Example 20A means string on 20 bytes
          x.slice(0, x.length - 1).toInt
        }
        case _ => {
          println(s"""
            FitsLib.getSplitLocation> Cannot infer size of type $shortType
            from the header! See com.sparkfits.FitsLib.getSplitLocation
              """)
          0
        }
      }
    }

    /**
      * Get the position (zero based) of a column with name `colName` of a HDU.
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @param colName : (String)
      *   The name of the column
      * @return (Int), position (zero-based) of the column.
      *
      */
    def getColumnPos(keyValues : Map[String, String], colName : String) : Int = {
      // Get the position of the column. Header names are TTYPE#
      val pos = Try {
        keyValues.filter(x => x._1.contains("TTYPE"))
          .map(x => (x._1, x._2.split("'")(1).trim()))
          .filter(x => x._2.toLowerCase == colName.toLowerCase)
          .keys.head.substring(5).toInt
      }.getOrElse(-1)

      val isCol = pos >= 0
      isCol match {
        case true => isCol
        case false => throw new AssertionError(s"""
          $colName is not a valid column name!
          """)
      }

      // Zero based
      pos - 1
    }

    // /**
    //   * Initialisation of the Bintable HDU.
    //   *
    //   * @param empty_hdu : (Boolean)
    //   *   Whether the HDU data block is empty.
    //   * @param header : (Array[String])
    //   *   Header of the HDU.
    //   * @param selectedColumns : (List[String])
    //   *   User can specify which columns to load. Default is all.
    //   *
    //   */
    // def initialize(empty_hdu: Boolean, header : Array[String],
    //     selectedColumns: List[String] = null): BintableHDU = {
    //
    //   val keyValues = FitsLib.parseHeader(header)
    //
    //   // Check if the user specifies columns to select
    //   this.colNames = keyValues.
    //     filter(x => x._1.contains("TTYPE")).
    //     map(x => (x._1, x._2.split("'")(1).trim()))
    //
    //   this.selectedColNames = if (selectedColumns != null) {
    //     selectedColumns
    //   } else {
    //     this.colNames.values.toList.asInstanceOf[List[String]]
    //   }
    //
    //   this.colPositions = selectedColNames.map(
    //     x => getColumnPos(keyValues, x)).toList.sorted
    //
    //   this.rowTypes = getColTypes(keyValues)
    //   // this.rowTypes = if (empty_hdu) {
    //   //   List[String]()
    //   // } else getColTypes(keyValues)
    //   val ncols = rowTypes.size
    //
    //   // splitLocations is an array containing the location of elements
    //   // (byte index) in a row. Example if we have a row with [20A, E, E], one
    //   // will have splitLocations = [0, 20, 24, 28] that is a string on 20 Bytes,
    //   // followed by 2 floats on 4 bytes each.
    //   this.splitLocations = (0 :: rowSplitLocations(rowTypes, 0)).scan(0)(_ +_).tail
    //   // this.splitLocations = if (empty_hdu) {
    //   //   List[Int]()
    //   // } else {
    //   //   (0 :: rowSplitLocations(rowTypes, 0)).scan(0)(_ +_).tail
    //   // }
    //
    //   this
    // }
  }
}
