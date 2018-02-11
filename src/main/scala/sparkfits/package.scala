package sparkfits

import org.apache.spark.sql.{DataFrameReader, DataFrame, Row, SparkSession}
import nom.tam.fits.{Fits, BinaryTableHDU, BinaryTable}
import org.apache.spark.rdd.RDD

package object fits {

  /**
   * Adds a method, `fitsFile`, to SQLContext that allows reading FITS data.
   */
  // implicit class FitsContext(spark : SparkSession) extends Serializable {
  implicit class FitsContext(spark : SparkSession) extends Serializable {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    def fits(fn : String) : DataFrame = {
      val nrows = 100
      val nBlock = 100
      val sizeBlock : Int = nrows / nBlock
      val nParts = 100
      val indexHDU = 1

      // Initialisation (1st column)
      val rdd = spark.sparkContext.parallelize(0 to nBlock - 1, nParts)
        .map(blockid => (blockid, new Fits(fn) with Serializable ))
        .map(x => yieldRows(x._2, indexHDU, x._1, sizeBlock, nrows))
        .flatMap(x => x)
      rdd.toDF()
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
    private def yieldRows(x : Fits,
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

    private def yieldRows2(x : Fits,
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

      val nColMax = 4

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
      // Map to Row to allow the conversion to DF later on
      ).map { x => Row.fromSeq(x)}.toList(0)) // Need a better handling of that...
    }

  }

  implicit class FitsDfr(dfr : DataFrameReader) extends Serializable {
  // class FitsDfr(sparkSession : SparkSession) extends DataFrameReader {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    def fits(fn : String) : DataFrame = {
      val nrows = 100
      val nBlock = 100
      val sizeBlock : Int = nrows / nBlock
      val nParts = 100
      val indexHDU = 1

      // Initialisation (1st column)
      val rdd = spark.sparkContext.parallelize(0 to nBlock - 1, nParts)
        .map(blockid => (blockid, new Fits(fn) with Serializable ))
        .map(x => yieldRows(x._2, indexHDU, x._1, sizeBlock, nrows))
        .flatMap(x => x)
      rdd.toDF()
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
    private def yieldRows(x : Fits,
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

    private def yieldRows2(x : Fits,
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

      val nColMax = 4

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
      // Map to Row to allow the conversion to DF later on
      ).map { x => Row.fromSeq(x)}.toList(0)) // Need a better handling of that...
    }
  }
}
