package sparkfits

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, DataFrame, Row, SparkSession}

import nom.tam.fits.{Fits, BinaryTableHDU}

import sparkfits.FitsSchema._
import sparkfits.SparkFitsUtil._

package object fits {

  /**
   * Adds a method, `fitsFile`, to SQLContext that allows reading FITS data.
   */
  // implicit class FitsContext(spark : SparkSession) extends Serializable {
  implicit class FitsContext(spark : SparkSession) extends Serializable {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    private val extraOptions = new scala.collection.mutable.HashMap[String, String]

    // Replace spark.read.format("fits") -> spark.readfits
    def readfits : FitsContext = FitsContext.this

    // Various options
    def option(key: String, value: String) : FitsContext = {
      FitsContext.this.extraOptions += (key -> value)
      FitsContext.this
    }

    def option(key: String, value: Boolean): FitsContext = {
      option(key, value.toString)
    }

    def option(key: String, value: Long): FitsContext = {
      option(key, value.toString)
    }

    def option(key: String, value: Double): FitsContext = {
      option(key, value.toString)
    }

    // Load the DF
    def load(fn : String) : DataFrame = {
      val nBlock = 100
      val nParts = 100
      val indexHDU = if (extraOptions("HDU") != null) {
        extraOptions("HDU").toInt
      } else 1

      // Access meta data
      val f = new Fits(fn)
      val data = f.getHDU(indexHDU).asInstanceOf[BinaryTableHDU]

      // A few checks
      val numberOfHdus = getNHdus(f)
      if (indexHDU >= numberOfHdus) {
        System.err.println(s"HDU number $indexHDU does not exist!")
        System.exit(1)
      }

      // Get number of rows
      val nrows = data.getNRows
      val sizeBlock : Int = nrows / nBlock

      // Get the schema
      val schema = getSchema(data)

      // Check the header
      val it = data.getHeader.iterator
      val myheader = getMyHeader(it, "")
      myheader.split(",").foreach(println)

      if (extraOptions("printme") != null) println(extraOptions("printme"))

      // Distribute the data
      val rdd = spark.sparkContext.parallelize(0 to nBlock - 1, nParts)
        .map(blockid => (blockid, new Fits(fn) with Serializable ))
        .map(x => yieldRows(x._2, indexHDU, x._1, sizeBlock, nrows))
        .flatMap(x => x)

      // Return DataFrame with Schema
      spark.createDataFrame(rdd, schema)
    }

    /**
      * Returns a block of rows as a sequence of sql.Row.
      * Useful to turn RDD into DF.
      * /!\ return tuple of Any (you need a conversion later on,
      * providing the schema for example)
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

  // implicit class FitsDfr(dfr : DataFrameReader) extends Serializable {
  // // class FitsDfr(sparkSession : SparkSession) extends DataFrameReader {
  //   val spark = SparkSession
  //     .builder()
  //     .getOrCreate()
  //   // For implicit conversions like converting RDDs to DataFrames
  //   import spark.implicits._
  //
  //   def fits(fn : String) : DataFrame = {
  //     val nrows = 100
  //     val nBlock = 100
  //     val sizeBlock : Int = nrows / nBlock
  //     val nParts = 100
  //     val indexHDU = 1
  //
  //     // Initialisation (1st column)
  //     val rdd = spark.sparkContext.parallelize(0 to nBlock - 1, nParts)
  //       .map(blockid => (blockid, new Fits(fn) with Serializable ))
  //       .map(x => yieldRows(x._2, indexHDU, x._1, sizeBlock, nrows))
  //       .flatMap(x => x)
  //     rdd.toDF()
  //   }
  //
  //   /**
  //     * Returns a block of rows as a Vector of Tuples.
  //     * Useful to turn RDD into DF.
  //     * /!\ return tuple of Any (you need a conversion later on).
  //     *
  //     * @param x : nom.tam.fits.Fits
  //     *             Instance of Fits.
  //     * @param indexHDU : int
  //     *             The HDU to be read.
  //     * @param offset : int
  //     *             Initial position of the cursor (first row)
  //     * @param sizeBlock : int
  //     *             Number of row to read.
  //     * @param nRowMax : int
  //     *             Number total of rows in the HDU.
  //     */
  //   private def yieldRows(x : Fits,
  //       indexHDU : Int,
  //       offset : Int,
  //       sizeBlock : Int,
  //       nRowMax : Int) = {
  //
  //     // Start of the block
  //     val start = offset * sizeBlock
  //
  //     // End of the block
  //     val stop_tmp = (offset + 1) * sizeBlock - 1
  //     val stop = if (stop_tmp < nRowMax - 1) {
  //       stop_tmp
  //     } else {
  //       nRowMax - 1
  //     }
  //
  //     // Yield rows
  //     for {
  //       i <- start to stop
  //     } yield (
  //       // Get the data as Array[Table]
  //       Array(x.getHDU(indexHDU).asInstanceOf[BinaryTableHDU]
  //       // Get ith row as an Array[Object]
  //       .getRow(i)
  //       // Get each element inside Objects
  //       // /!\ Strings are not returned as Object...
  //       .map {
  //         case x : Array[_] => x.asInstanceOf[Array[_]](0)
  //         case x : String => x
  //       }
  //     // Map to tuple to allow the conversion to DF later on
  //     ).map {
  //       case x => (x(0).toString, x(1).toString, x(2).toString)
  //     }.toList(0)) // Need a better handling of that...
  //   }
  //
  //   private def yieldRows2(x : Fits,
  //       indexHDU : Int,
  //       offset : Int,
  //       sizeBlock : Int,
  //       nRowMax : Int) = {
  //
  //     // Start of the block
  //     val start = offset * sizeBlock
  //
  //     // End of the block
  //     val stop_tmp = (offset + 1) * sizeBlock - 1
  //     val stop = if (stop_tmp < nRowMax - 1) {
  //       stop_tmp
  //     } else {
  //       nRowMax - 1
  //     }
  //
  //     val nColMax = 4
  //
  //     // Yield rows
  //     for {
  //       i <- start to stop
  //     } yield (
  //       // Get the data as Array[Table]
  //       Array(x.getHDU(indexHDU).asInstanceOf[BinaryTableHDU]
  //       // Get ith row as an Array[Object]
  //       .getRow(i)
  //       // Get each element inside Objects
  //       // /!\ Strings are not returned as Object...
  //       .map {
  //         case x : Array[_] => x.asInstanceOf[Array[_]](0)
  //         case x : String => x
  //       }
  //     // Map to Row to allow the conversion to DF later on
  //     ).map { x => Row.fromSeq(x)}.toList(0)) // Need a better handling of that...
  //   }
  // }
}
