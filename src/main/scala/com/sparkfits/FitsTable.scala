package com.sparkfits

import java.io.IOError
import java.nio.ByteBuffer
import java.io.EOFException
import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._

import scala.util.{Try, Success, Failure}

/**
  * This is the beginning of a FITS library in Scala.
  * You will find a large number of methodes to manipulate Binary Table HDUs.
  * There is no support for image HDU for the moment.
  */
object FitsTableLib {
  case class TableInfos() extends FitsLib.Infos {

    def implemented: Boolean = {false}

    def getNRows(keyValues: Map[String, String]) : Long = {0L}
    def getSizeRowBytes(keyValues: Map[String, String]) : Int = {0}
    def getNCols(keyValues : Map[String, String]) : Long = {0L}
    def getColTypes(keyValues : Map[String, String]): List[String] = {null}

    def listOfStruct : List[StructField] = {
      // Get the list of StructField.
      val lStruct = List.newBuilder[StructField]
      /*
      for (col <- colPositions) {
        lStruct += readMyType(colNames(col), rowTypes(col))
      }
      */
      lStruct.result
    }

    def getRow(buf: Array[Byte]): List[Any] = {
      var row = List.newBuilder[Any]

      row.result
    }

    def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {null}

    def readMyType(name : String, fitstype : String, isNullable : Boolean = true): StructField = {
      fitstype match {
        case x if fitstype.contains("A") => StructField(name, StringType, isNullable)
        case _ => {
          println(s"""FitsTable.readMyType> Cannot infer type $fitstype from the header!
            See com.sparkfits.FitsSchema.scala
            """)
          StructField(name, StringType, isNullable)
        }
      }
    }
  }
}
