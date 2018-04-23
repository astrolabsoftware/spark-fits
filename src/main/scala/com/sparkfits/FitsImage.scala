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
object FitsImageLib {
  case class ImageInfos(pixelSize: Int, axis: Array[Long]) extends FitsLib.Infos {
    def getRow(buf: Array[Byte]): List[Any] = {
      var row = List.newBuilder[Any]

      row += pixelSize
      row += axis
      row += buf

      println(s"FitsImageLib.ImageInfos.getRow> pixelSize=$pixelSize axis=${axis.toString} buf=${buf.slice(0, 10).toString}")

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
      1L
    }

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

    def getNCols(header : Array[String]) : Long = {
      val keyValues = FitsLib.parseHeader(header)
      0L
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

    def listOfStruct : List[StructField] = {
      // Get the list of StructField.
      val lStruct = List.newBuilder[StructField]
      lStruct += StructField("PixelSize", IntegerType, true)
      lStruct += StructField("Axis", ArrayType(LongType, true), true)
      lStruct += StructField("Image", ArrayType(ByteType, true), true)
      lStruct.result
    }
  }
}
