package sparkfits

object fitsSchema {

  def ReadMyType(name : String, fitstype : String): StructField = {
    fitstype match {
      case "1J" => StructField(name, IntegerType, false)
      case "1E" => StructField(name, DoubleType, false)
      case _ => StructField(name, StringType, false)
    }
  }

  def ListOfStruct(data : BinaryTableHDU, col : Int, colmax : Int) : List[StructField] = {
    if (col == colmax)
      Nil
    else
      ReadMyType(data.getColumnName(col), data.getColumnFormat(col)) :: ListOfStruct(data, col + 1, colmax)
  }
  
}
