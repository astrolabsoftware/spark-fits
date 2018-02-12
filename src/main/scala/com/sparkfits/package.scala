package com.sparkfits

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, DataFrame, Row, SparkSession}

import nom.tam.fits.{Fits, BinaryTableHDU}

import com.sparkfits.FitsSchema._
import com.sparkfits.SparkFitsUtil._

package object fits {

  /**
   * Adds a method, `fitsFile`, to SparkSession that allows reading FITS data.
   * Note that for the moment, we provide support only for FITS table.
   * We will add FITS image later on.
   *
   * The interpreter session below shows how to use basic functionalities:
   *
   * {{{
   * scala> val fn = "/path/to/myfits"
   * scala> val df = spark.readfits
   *  .option("datatype", "table")
   *  .option("HDU", 1)
   *  .option("printHDUHeader", true)
   *  .load(fn)
   * +------ HEADER (HDU=1) ------+
   * ...
   * +----------------------------+
   * df: org.apache.spark.sql.DataFrame = [...]
   *
   * scala> df.printSchema
   * root
   *  |-- var1: integer (nullable = false)
   *  |-- var2: float (nullable = false)
   *  |-- var3: double (nullable = false)
   *
   * }}}
   * TODO: Add test file in resource.
   */
  // implicit class FitsContext(spark : SparkSession) extends Serializable {
  implicit class FitsContext(spark : SparkSession) extends Serializable {

    // This will contain all options use to load the data
    private val extraOptions = new scala.collection.mutable.HashMap[String, String]

    /**
      * Replace the current syntax in spark 2.X
      * spark.read.format("fits") --> spark.readfits
      * This is a hack to avoid touching DataFrameReader class, for which the
      * constructor is private... If you have a better idea, bug me!
      *
      * @return FitsContext
      */
    def readfits : FitsContext = FitsContext.this

    /**
      * Adds an input options for reading the underlying data source.
      *
      * In general you can set the following option(s):
      * - option("HDU", <Int>)
      * - option("datatype", <String>)
      * - option("printHDUHeader", <Boolean>)
      *
      * Note that values pass as Boolean, Long, or Double will be first
      * converted to String and then decoded later on.
      *
      * @param key : (String)
      *   Name of the option
      * @param value : (String)
      *   Value of the option.
      */
    def option(key: String, value: String) : FitsContext = {
      FitsContext.this.extraOptions += (key -> value)
      FitsContext.this
    }

    /**
      * Adds an input options for reading the underlying data source.
      * (key, boolean)
      *
      * @param key : (String)
      *   Name of the option
      * @param value : (Boolean)
      *   Value of the option.
      */
    def option(key: String, value: Boolean): FitsContext = {
      option(key, value.toString)
    }

    /**
      * Adds an input options for reading the underlying data source.
      * (key, Long)
      *
      * @param key : (String)
      *   Name of the option
      * @param value : (Long)
      *   Value of the option.
      */
    def option(key: String, value: Long): FitsContext = {
      option(key, value.toString)
    }

    /**
      * Adds an input options for reading the underlying data source.
      * (key, Double)
      *
      * @param key : (String)
      *   Name of the option
      * @param value : (Double)
      *   Value of the option.
      */
    def option(key: String, value: Double): FitsContext = {
      option(key, value.toString)
    }

    /** Load a BinaryTableHDU data contained in one HDU as a DataFrame.
      * The schema of the DataFrame is directly inferred from the
      * header of the fits HDU.
      *
      * @param fn : (String)
      *  Path + filename of the fits file to be read
      * @return : DataFrame
      */
    def load(fn : String) : DataFrame = {
      // Partitioning of the data
      val nBlock = 100
      val nParts = 100

      // Check that you can read the data!
      val dataType = if (extraOptions.contains("datatype")) {
        extraOptions("datatype")
      } else {
        System.err.println("""
          You did not specify the data type!
          Please choose one of the following:
            spark.readfits.option("datatype", "table")
            spark.readfits.option("datatype", "image")
          """)
        System.exit(1)
      }

      // Check that the user specifies table
      if (extraOptions("datatype") != "table") {
        System.err.println("""
          Currently only reading data from table is supported.
          Support for image data will be added later.
          Please use spark.readfits.option("datatype", "table")
          """)
        System.exit(1)
      }

      // Check that the user specifies the HDU number
      if (!extraOptions.contains("HDU")) {
        System.err.println(s"""
          You need to specify the HDU to be read!
          spark.readfits.option("HDU", <Int>)
          """)
        System.exit(1)
      }

      // Open the file
      val f = new Fits(fn)

      // Grab the desired HDU number
      val indexHDU = extraOptions("HDU").toInt

      // Check the number of HDUs
      val numberOfHdus = getNHdus(f)
      if (indexHDU >= numberOfHdus) {
        System.err.println(s"""
          HDU number $indexHDU does not exist!
          """)
        System.exit(1)
      }

      // Access the meta data
      val data = f.getHDU(indexHDU).asInstanceOf[BinaryTableHDU]

      // Get number of rows
      val nrows = data.getNRows
      val sizeBlock : Int = nrows / nBlock

      // Get the schema
      val schema = getSchema(data)

      // Check the header
      if (extraOptions.contains("printHDUHeader")) {
        if (extraOptions("printHDUHeader").toBoolean) {
          val it = data.getHeader.iterator
          val myheader = getMyHeader(it, "")
          println(s"+------ HEADER (HDU=$indexHDU) ------+")
          myheader.split(",").foreach(println)
          println("+----------------------------+")
        }
      }

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
      * @param x : (nom.tam.fits.Fits)
      *   Instance of Fits.
      * @param indexHDU : (Int)
      *   The HDU to be read.
      * @param offset : (int)
      *   Initial position of the cursor (first row)
      * @param sizeBlock : (Int)
      *   Number of row to read.
      * @param nRowMax : (Int)
      *   Number total of rows in the HDU.
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
      // Map to Row to allow the conversion to DF later on
      ).map { x => Row.fromSeq(x)}.toList(0)) // Need a better handling of that...
    }
  }
}
