package com.sparkfits

import java.io.IOError
import java.nio.ByteBuffer
import java.io.EOFException
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.types._

/**
  * This is the beginning of a FITS library in Scala.
  * You will find a large number of methodes to manipulate Binary Table HDUs.
  * There is no support for image HDU for the moment.
  */
object FitsBintableLib {
  case class BintableInfos() extends FitsLib.Infos {

    var rowTypes: List[String] = List()
    var colNames: Map[String, String] = Map()
    var selectedColNames: List[String] = List()
    var colPositions: List[Int] = List()
    var splitLocations: List[Int] = List()

    def initialize(_rowTypes: List[String],
                   _colNames: Map[String, String],
                   _selectedColNames: List[String],
                   _colPositions: List[Int],
                   _splitLocations: List[Int]) = {
      rowTypes = _rowTypes
      this.colNames = _colNames
      this.selectedColNames = _selectedColNames
      this.colPositions = _colPositions
      this.splitLocations = _splitLocations
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

    def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {
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
          println(s"""FitsLib.getElementFromBuffer> Cannot infer size of type $shortType from the header!
              See getElementFromBuffer
              """)
          0
        }
      }
    }

    def getRow(buf: Array[Byte]): List[Any] = {
      var row = List.newBuilder[Any]

      for (col <- colPositions) {
        row += getElementFromBuffer(
          buf.slice(splitLocations(col), splitLocations(col+1)), rowTypes(col))
      }
      row.result
    }

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
    def getNRows(header : Array[String]) : Long = {
      val keyValues = FitsLib.parseHeader(header)
      keyValues("NAXIS2").toLong
    }

    /**
      * Get the number of column of a HDU.
      * We rely on what's written in the header, meaning
      * here we do not access the data directly.
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @return (Long), the number of rows as written in KEYWORD=TFIELDS.
      *
      */
    def getNCols(header : Array[String]) : Long = {
      val keyValues = FitsLib.parseHeader(header)
      // println(s"getNCols> keyValues=${keyValues.toString}")
      if (keyValues.contains("TFIELDS")) keyValues("TFIELDS").toLong
      else 0L
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
    def getColTypes(header : Array[String]): List[String] = {
      // Get the names of the Columns
      val keyValues = FitsLib.parseHeader(header)

      val colTypes = List.newBuilder[String]

      val ncols = getNCols(header).toInt

      for (col <- 0 to ncols-1) {
        colTypes += FitsLib.shortStringValue(keyValues("TFORM" + (col + 1).toString))
      }
      colTypes.result
    }

    def readMyType(name : String, fitstype : String, isNullable : Boolean = true): StructField = {
      fitstype match {
        case x if fitstype.contains("I") => StructField(name, ShortType, isNullable)
        case x if fitstype.contains("J") => StructField(name, IntegerType, isNullable)
        case x if fitstype.contains("K") => StructField(name, LongType, isNullable)
        case x if fitstype.contains("E") => StructField(name, FloatType, isNullable)
        case x if fitstype.contains("D") => StructField(name, DoubleType, isNullable)
        case x if fitstype.contains("L") => StructField(name, BooleanType, isNullable)
        case x if fitstype.contains("A") => StructField(name, StringType, isNullable)
        case _ => {
          println(s"""FitsBintable.readMyType> Cannot infer type $fitstype from the header!
            See com.sparkfits.FitsSchema.scala
            """)
          StructField(name, StringType, isNullable)
        }
      }
    }

    def listOfStruct : List[StructField] = {
      // Grab max number of column
      // Get the list of StructField.
      val lStruct = List.newBuilder[StructField]

      val cns = List.newBuilder[String]
      for (colIndex <- colPositions) {
        cns += colNames("TTYPE" + (colIndex + 1).toString)
      }

      // println(s"BintableInfos.listOfStruct> rowTypes=${rowTypes.toString} colPositions=${colPositions.toString} cns=${cns.toString}")

      for (colIndex <- colPositions) {
        val colName = FitsLib.shortStringValue(colNames("TTYPE" + (colIndex + 1).toString))

        // println(s"listOfStruct> colname=${colName} rowType=${rowTypes(colIndex)}")

        lStruct += readMyType(colName, rowTypes(colIndex))
      }

      lStruct.result

    }

  }
}
