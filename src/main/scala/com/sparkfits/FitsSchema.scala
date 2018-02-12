package com.sparkfits

import org.apache.spark.sql.types._

import nom.tam.fits.BinaryTableHDU

object FitsSchema {

  /**
    * http://archive.stsci.edu/fits/users_guide/node47.html#SECTION00563000000000000000
    */
  def ReadMyType(name : String, fitstype : String): StructField = {
    fitstype match {
      case "1J" => StructField(name, IntegerType, true)
      case "1E" => StructField(name, FloatType, true)
      case "E" => StructField(name, FloatType, true)
      case "L" => StructField(name, BooleanType, true)
      case "D" => StructField(name, DoubleType, true)
      case _ => StructField(name, StringType, true)
    }
  }

  def ListOfStruct(data : BinaryTableHDU, col : Int, colmax : Int) : List[StructField] = {
    if (col == colmax)
      Nil
    else
      ReadMyType(data.getColumnName(col), data.getColumnFormat(col)) :: ListOfStruct(data, col + 1, colmax)
  }

  def getSchema(data : BinaryTableHDU) : StructType = {
    val ncols = data.getNCols
    StructType(ListOfStruct(data, 0, ncols))
  }

}
