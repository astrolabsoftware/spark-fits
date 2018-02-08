package sparkfits

import org.apache.spark.sql.{SparkSession, DataFrame, SQLContext, Row}
import org.apache.spark.sql.types._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.rdd.RDD

import org.apache.log4j.Level
import org.apache.log4j.Logger

import nom.tam.fits.{Fits, HeaderCard, BinaryTableHDU, Header, BinaryTable}
import nom.tam.util.{Cursor, BufferedFile, ArrayDataInput}

object ReadFits {

  // Set to Level.WARN is you want verbosity
  // Logger.getLogger("org").setLevel(Level.OFF)
  // Logger.getLogger("akka").setLevel(Level.OFF)

  // Initialise Spark context
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sq = new SQLContext(sc)
  val spark = SparkSession
    .builder()
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  // Get the number of HDUs
  def getNHdus(f : Fits, n : Int) : Int = {
    if (f.getHDU(n) != null) getNHdus(f, n + 1) else n
  }

  // Get the header
  def getMyHeader(c : Cursor[String, HeaderCard], s : String) : String = {
    if (c.hasNext() == true) getMyHeader(c, s + c.next() + ",") else s
  }

  def header(h : Header) = {
    val c = h.iterator()
    do {
      val card = c.next()
      val key = card.getKey
      try {
        val valueType = card.valueType
        // println("===key", key)
        val typ = key match {
          case "END" => ""
          case _ => card.valueType.getCanonicalName
        }
        val value = key match {
          case "END" => ""
          case _ => card.getValue.toString
        }
        // println(s"  key=$key type=$typ value=$value")
      } catch {
        case e:Exception =>
      }
    } while (c.hasNext)
  }

  /**
    * Returns a block of rows as a Vector of Tuples.
    * Useful to turn RDD into DF.
    * /!\ return tuple of Any (you need a conversion later on).
    *
    * @param x : nom.tam.fits.Fits
    *             Instance of Fits.
    * @param indexHDU : int
    *             The HDU to be read.
    * @param offset : int
    *             Initial position of the cursor (first row)
    * @param sizeBlock : int
    *             Number of row to read.
    * @param nRowMax : int
    *             Number total of rows in the HDU.
    */
  def yieldRows(x : Fits,
      indexHDU : Int,
      offset : Int,
      sizeBlock : Int,
      nRowMax : Int) = {

    // Start of the block
    val start = offset * sizeBlock

    // End of the block
    val stop_tmp = (offset + 1) * sizeBlock - 1
    val stop = if (stop_tmp < nRowMax - 1) {
      stop_tmp
    } else {
      nRowMax - 1
    }

    // Yield rows
    for {
      i <- start to stop
    } yield (
      // Get the data as Array[Table]
      Array(x.getHDU(indexHDU).asInstanceOf[BinaryTableHDU]
      // Get ith row as an Array[Object]
      .getRow(i)
      // Get each element inside Objects
      // /!\ Strings are not returned as Object...
      .map {
        case x : Array[_] => x.asInstanceOf[Array[_]](0)
        case x : String => x
      }
    // Map to tuple to allow the conversion to DF later on
    ).map {
      case x => (x(0).toString, x(1).toString, x(2).toString)
    }.toList(0)) // Need a better handling of that...
  }

  def main(args : Array[String]) {
    // Open the fits
    val ffits = new Fits(args(0).toString) with Serializable

    // Get the total number of HDUs
    val nHDUs = getNHdus(ffits, 0)

    println("Number of HDUs : " + nHDUs.toString)

    // Process only HDU > 0
    for (indexHDU <- 1 to nHDUs - 1) {
      println(s"Processing HDU number $indexHDU")

      // Get the hdu
      val hdu = ffits.getHDU(indexHDU)

      // Check it is a table
      val isTable = hdu.isInstanceOf[BinaryTableHDU]
      if (isTable == false) {
        System.err.println("Work only with BinaryTableHDU!")
        System.exit(1)
      }

      val data = hdu.asInstanceOf[BinaryTableHDU]

      val ncols = data.getNCols
      val nrows = data.getNRows
      println(s"Table with [col x row] = [$ncols x $nrows]")

      val nBlock = 100
      val sizeBlock : Int = nrows / nBlock
      val nParts = 100
      val col0 = 0

      val it = data.getHeader.iterator
      val myheader = getMyHeader(it, "")

      println(myheader)

      println(data.getColumnName(0))
      println(data.getColumnName(1))
      println(data.getColumnFormat(0))
      println(data.getColumnFormat(1))
      val fitsSchema = StructType(
        List(
          StructField("ra", DoubleType, false),
          StructField("dec", DoubleType, false)
        )
      )

      def yieldEmptyRows(offset : Int, sizeBlock : Int) = {

        // Start of the block
        val start = offset * sizeBlock

        // End of the block
        val stop_tmp = (offset + 1) * sizeBlock - 1
        val stop = if (stop_tmp < nrows - 1) {
          stop_tmp
        } else {
          nrows - 1
        }

        // Yield rows
        for {
          i <- start to stop
        } yield (i)
      }

      def getDF(rdd : RDD[(Any, Int)], fitstype : String, name : String) = {
        val x = fitstype match {
          case "1J" => rdd.asInstanceOf[RDD[(Int, Int)]].toDF(name, "id")
          case "1E" => rdd.asInstanceOf[RDD[(Double, Int)]].toDF(name, "id")
          case "D" => rdd.asInstanceOf[RDD[(Double, Int)]].toDF(name, "id")
          case _ => rdd.asInstanceOf[RDD[(Float, Int)]].toDF(name, "id")
        }
        x
      }

      // def recurDF(df : DataFrame, col : Int, colmax : Int) : DataFrame = {
      //   if (col == colmax + 1) {
      //     df
      //   } else {
      //     val rdd_tmp = sc.parallelize(0 to nBlock - 1, nParts)
      //       .map(blockid => (blockid, new Fits(args(0).toString) with Serializable ))
      //       .map(x => yieldRows(x._2, x._1, sizeBlock, nrows).zip(yieldEmptyRows(x._1, sizeBlock)))
      //       .flatMap(x => x)
      //
      //     val fitstype : String = data.getColumnFormat(col)
      //     val df_tmp = getDF(rdd_tmp, fitstype, col.toString)
      //
      //     println(df_tmp.count())
      //     println(df.join(df_tmp, "id").count())
      //     recurDF(df.join(df_tmp, "id"), col + 1, colmax)
      //   }
      // }

      // Initialisation (1st column)
      val rdd = sc.parallelize(0 to nBlock - 1, nParts)
        .map(blockid => (blockid, new Fits(args(0).toString) with Serializable ))
        .map(x => yieldRows(x._2, indexHDU, x._1, sizeBlock, nrows))
        .flatMap(x => x)

      val df = rdd.toDF()
      df.show()
      df.printSchema()
      // Get the correct type for the column
      // Type of the data
      // val fitstype : String = data.getColumnFormat(col0)
      // val df = getDF(rdd, fitstype, col0.toString)
      // val rddtyped = getRDDType(rdd, fitstype)
      // println("Initial length = " + rddtyped.count().toString)

      // Recursion
      // val df_tot = recurDF(df, 1, 2)

      // Finalize
      // println(df_tot.select("1").count())
      // df_tot.show()

    }
  }
}
