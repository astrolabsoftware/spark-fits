package sparkfits

import org.apache.spark.sql.types._

import nom.tam.fits.BinaryTableHDU

object FitsSchema {

  def ReadMyType(name : String, fitstype : String): StructField = {
    fitstype match {
      case "1J" => StructField(name, IntegerType, false)
      case "1E" => StructField(name, FloatType, false)
      case "D" => StructField(name, DoubleType, false)
      case _ => StructField(name, StringType, false)
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
