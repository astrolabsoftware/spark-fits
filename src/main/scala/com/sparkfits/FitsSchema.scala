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

import com.sparkfits.FitsBlock._

/**
  * Object to handle the conversion from a HDU header to a DataFrame Schema.
  */
object FitsSchema_new {

  /**
    * Conversion from fits type to DataFrame Schema type.
    * This can be used to set the name of a column and the type of elements
    * in that column. Fits types nomenclature explained here:
    * http://archive.stsci.edu/fits/users_guide/node47.html#SECTION00563000000000000000
    *
    * @param name : (String)
    *   The name of the future column in the DataFrame
    * @param fitstype : (String)
    *   The type of elements from the fits HEADER. See the link provided.
    * @return a `StructField` containing name, type and isNullable informations.
    *
    */
  def ReadMyType(name : String, fitstype : String, isNullable : Boolean = true): StructField = {
    fitstype match {
      case "1J" => StructField(name, IntegerType, isNullable)
      case "1E" => StructField(name, FloatType, isNullable)
      case "E" => StructField(name, FloatType, isNullable)
      case "L" => StructField(name, BooleanType, isNullable)
      case "D" => StructField(name, DoubleType, isNullable)
      case _ => StructField(name, StringType, isNullable)
    }
  }

  /**
    * Construct a list of `StructField` to be used to construct a DataFrame Schema.
    * this routine should be used recursively. By default it includes all columns.
    *
    * @param data : (BinaryTableHDU)
    *   The HDU data containing informations about the column name and element types.
    * @param col : (Int)
    *   The index of the column.
    * @param colmax : (Int)
    *   The total number of columns in the HDU.
    * @return a `List[StructField]` with informations about name and type for all columns.
    */
  def ListOfStruct(fB : FitsBlock, col : Int) : List[StructField] = {
    fB.resetCursorAtHeader
    val header = fB.readHeader
    val colmax = fB.getNCols(header)
    if (col == colmax)
      Nil
    else
      ReadMyType(fB.getColumnName(header, col), fB.getColumnType(header, col)) :: ListOfStruct(fB, col + 1)
  }

  /**
    * Retrieve DataFrame Schema from HDU header.
    *
    * @param data : (BinaryTableHDU)
    *   The HDU data containing informations about the column name and element types.
    * @return Return a `StructType` which contain a list of `StructField` with informations about name and type for all columns.
    *
    */
  def getSchema(fB : FitsBlock) : StructType = {
    fB.resetCursorAtHeader
    val header = fB.readHeader
    StructType(ListOfStruct(fB, 0))
  }
}
