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

import org.apache.spark.sql.types.StructField

import com.sparkfits.FitsHdu._

/**
  * Contain class and methods to manipulate Table HDU.
  */
object FitsHduTable {
  case class TableHDU() extends HDU {

    /** Not yet implemented */
    override def implemented: Boolean = {false}

    /** Zero-type, null type elements */
    override def getNRows(keyValues: Map[String, String]) : Long = {0L}
    override def getSizeRowBytes(keyValues: Map[String, String]) : Int = {0}
    override def getNCols(keyValues : Map[String, String]) : Long = {0L}
    override def getColTypes(keyValues : Map[String, String]): List[String] = {null}

    /** Empty schema. */
    override def listOfStruct : List[StructField] = {
      // Get the list of StructField.
      val lStruct = List.newBuilder[StructField]
      /*
      for (col <- colPositions) {
        lStruct += readMyType(colNames(col), rowTypes(col))
      }
      */
      lStruct.result
    }

    /** Return empty row */
    override def getRow(buf: Array[Byte]): List[Any] = {
      var row = List.newBuilder[Any]

      row.result
    }

    /** Return null elements */
    override def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {null}
  }
}
