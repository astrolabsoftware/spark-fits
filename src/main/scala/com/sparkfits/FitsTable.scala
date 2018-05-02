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

/**
  * This is the beginning of a FITS library in Scala.
  * You will find a large number of methodes to manipulate Binary Table HDUs.
  * There is no support for image HDU for the moment.
  */
object FitsTableLib {
  case class TableInfos() extends FitsLib.Infos {

    /** Not yet implemented */
    def implemented: Boolean = {false}

    /** Zero-type, null type elements */
    def getNRows(keyValues: Map[String, String]) : Long = {0L}
    def getSizeRowBytes(keyValues: Map[String, String]) : Int = {0}
    def getNCols(keyValues : Map[String, String]) : Long = {0L}
    def getColTypes(keyValues : Map[String, String]): List[String] = {null}

    /** Empty schema. */
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

    /** Return empty row */
    def getRow(buf: Array[Byte]): List[Any] = {
      var row = List.newBuilder[Any]

      row.result
    }

    /** Return null elements */
    def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {null}

    /**
      * Return DF schema-compatible structure according to Header informations.
      * Currently returning only String.
      *
      * @param name : (String)
      *   Name of the column
      * @param fitstype : (String)
      *   Type of the column elements according to the FITS header.
      * @param isNullable : (Boolean)
      *   Whether the DF entry is nullable. Default is True.
      *
      * @return (StructField) StructField containing column information. This
      *   StructField will be used later to build the schema of the DataFrame.
      *
      */
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
