package com.sparkfits

import java.io.IOError
import java.nio.ByteBuffer
import java.io.EOFException
import java.nio.charset.StandardCharsets

import scala.util.Random

import org.apache.spark.sql.types._

import com.sparkfits.FitsHdu._

/**
  * This is the beginning of a FITS library in Scala.
  * You will find a large number of methodes to manipulate Binary Table HDUs.
  * There is no support for image HDU for the moment.
  */
object FitsImageLib {
  case class ImageHDU(pixelSize: Int, axis: Array[Long]) extends HDU {

    def implemented: Boolean = {true}

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
    def getNRows(keyValues: Map[String, String]) : Long = {
      val totalBytes = axis.reduce(_ * _) * pixelSize
      val rowBytes = getSizeRowBytes(keyValues)

      val result = if (totalBytes % rowBytes == 0) {
        (totalBytes / rowBytes).toLong
      }
      else {
        ((totalBytes / rowBytes) + 1).toLong
      }

      println(s"FitsImageLib.ImageHDU.getNRows> result=$result")

      result
    }

    def getSizeRowBytes(keyValues: Map[String, String]) : Int = {
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

    def getNCols(keyValues : Map[String, String]) : Long = {
      // println(s"FitsImageLib.ImageHDU.getNCols> ")
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
    def getColTypes(keyValues : Map[String, String]): List[String] = {
      // println(s"FitsImageLib.ImageHDU.getColTypes> ")
      // Get the names of the Columns

      val colTypes = List.newBuilder[String]
      colTypes += "Image"
      colTypes.result
    }

    def listOfStruct : List[StructField] = {
      // println(s"FitsImageLib.ImageHDU.listOfStruct> ")
      // Get the list of StructField.

      val lStruct = List.newBuilder[StructField]
      lStruct += StructField("Image", ArrayType(ByteType, true))
      lStruct.result
    }

    def getRow(buf: Array[Byte]): List[Any] = {
      val row = List.newBuilder[Any]
      row += buf
      row.result
    }

    def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {
      // println(s"FitsImageLib.ImageHDU.getElementFromBuffer> ")

      subbuf
    }
  }
}
