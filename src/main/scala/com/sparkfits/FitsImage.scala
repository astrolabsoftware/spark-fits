package com.sparkfits

import java.io.IOError
import java.nio.ByteBuffer
import java.io.EOFException
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.types._

import scala.util.Random

/**
  * This is the beginning of a FITS library in Scala.
  * You will find a large number of methodes to manipulate Binary Table HDUs.
  * There is no support for image HDU for the moment.
  */
object FitsImageLib {
  case class ImageInfos(pixelSize: Int, axis: Array[Long]) extends FitsLib.Infos {

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
      // println(s"FitsImageLib.ImageInfos.getNRows> ")
      ((axis.reduce(_ * _) * pixelSize) / getSizeRowBytes(keyValues)) + 1
    }

    def getSizeRowBytes(keyValues: Map[String, String]) : Int = {
      // println(s"FitsImageLib.ImageInfos.getSizeRowBytes> ")
      1024
    }

    def getNCols(keyValues : Map[String, String]) : Long = {
      // println(s"FitsImageLib.ImageInfos.getNCols> ")
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
      // println(s"FitsImageLib.ImageInfos.getColTypes> ")
      // Get the names of the Columns

      val colTypes = List.newBuilder[String]
      colTypes += "Image"
      colTypes.result
    }

    def listOfStruct : List[StructField] = {
      // println(s"FitsImageLib.ImageInfos.listOfStruct> ")
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
      // println(s"FitsImageLib.ImageInfos.getElementFromBuffer> ")

      subbuf
    }
  }
}
