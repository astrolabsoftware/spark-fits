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

import com.sparkfits.FitsLib.Fits

/**
  * Object to handle the conversion from a HDU header to a DataFrame Schema.
  */
object FitsSchema {

  /**
    * Conversion from fits type to DataFrame Schema type.
    * This can be used to set the name of a column and the type of elements
    * in that column. Fits types nomenclature explained here:
    * https://fits.gsfc.nasa.gov/standard30/fits_standard30.pdf
    *
    * @param name : (String)
    *   The name of the future column in the DataFrame
    * @param fitstype : (String)
    *   The type of elements from the fits HEADER. See the link provided.
    * @param isNullable : (Boolean)
    *   Column is nullable if True (default).
    * @return a `StructField` containing name, type and isNullable informations.
    *
    */
  def ReadMyType(name : String, fitstype : String, isNullable : Boolean = true): StructField = {
    fitstype match {
      case x if fitstype.contains("I") => StructField(name, ShortType, isNullable)
      case x if fitstype.contains("J") => StructField(name, IntegerType, isNullable)
      case x if fitstype.contains("K") => StructField(name, LongType, isNullable)
      case x if fitstype.contains("E") => StructField(name, FloatType, isNullable)
      case x if fitstype.contains("D") => StructField(name, DoubleType, isNullable)
      case x if fitstype.contains("L") => StructField(name, BooleanType, isNullable)
      case x if fitstype.contains("A") => StructField(name, StringType, isNullable)
      case _ => {
        println(s"""FitsSchema.ReadMyType> Cannot infer type $fitstype from the header!
            See com.sparkfits.FitsSchema.scala
            """)
        StructField(name, StringType, isNullable)
      }
    }
  }

  /**
    * Construct a list of `StructField` to be used to construct a DataFrame Schema.
    * This routine is recursive. By default it includes all columns.
    *
    * @param fits : (Fits)
    *   Fits instance.
    * @param col : (Int)
    *   The index of the column used for the recursion. Should be left at 0.
    * @return a `List[StructField]` with informations about name and type for all columns.
    */
  def ListOfStruct(fits : Fits, col : Int = 0) : List[StructField] = {
    // Reset the cursor at header
    fits.resetCursorAtHeader

    // Read the header
    val header = fits.blockHeader
    checkAnyHeader(header)

    if (fits.hdu.implemented){
      fits.hdu.listOfStruct
    }
    else {
      List[StructField]()
    }
  }

  /**
    * Retrieve DataFrame Schema from HDU header.
    *
    * @param fits : (Fits)
    *   Fits instance
    * @return Return a `StructType` which contain a list of `StructField`
    *   with informations about name and type for all columns.
    *
    */
  def getSchema(fits : Fits) : StructType = {
    // Construct the schema from the header.
    StructType(ListOfStruct(fits))
  }

  /**
    * Return schema for empty DataFrame
    *
    * @return Return a `StructType` with one entry stating nothing.
    *
    */
  def getEmptySchema : StructType = {
    // Construct empty schema
    StructType(StructField("empty", StringType, true) :: Nil)
  }

  /**
    * A few checks on the header for any header type
    *
    * @param header : (Array[String])
    *   The header of the HDU.
    */
  def checkAnyHeader(header : Array[String]) : Unit = {

    // Check that we have an extension
    val keysHasXtension = header(0).contains("XTENSION")
    keysHasXtension match {
      case true => keysHasXtension
      case false => throw new AssertionError("""
        Your header has no keywords called XTENSION.
        Check that the HDU number you want to
        access is correct: spark.readfits.option("HDU", <Int>).
        """)
    }

    // Check that header end.
    val headerEND = header.reverse(0).contains("END")
    headerEND match {
      case true => headerEND
      case false => throw new AssertionError("""
        There is a problem with your HEADER. It should end with END.
        Is it a standard header of size 2880 bytes? You should check it
        using the option spark.readfits.option("verbose", true).
        """)
    }
  }
}
