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

import scala.util.{Try, Success, Failure}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

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
   * scala> val fn = "src/test/resources/test.fits"
   * scala> val df = spark.readfits
   *  .option("datatype", "table")
   *  .option("HDU", 1)
   *  .option("printHDUHeader", true)
   *  .load(fn)
   * +------ HEADER (HDU=1) ------+
   * XTENSION= BINTABLE             / binary table extension
   * BITPIX  =                    8 / array data type
   * NAXIS   =                    2 / number of array dimensions
   * NAXIS1  =                   32 / length of dimension 1
   * NAXIS2  =                  100 / length of dimension 2
   * PCOUNT  =                    0 / number of group parameters
   * GCOUNT  =                    1 / number of groups
   * TFIELDS =                    4 / number of table fields
   * TTYPE1  = target
   * TFORM1  = 20A
   * TTYPE2  = RA
   * TFORM2  = E
   * TTYPE3  = Dec
   * TFORM3  = E
   * TTYPE4  = Redshift
   * TFORM4  = E
   * END
   * +----------------------------+
   * df: org.apache.spark.sql.DataFrame = [target: string, RA: float ... 2 more fields]
   *
   * scala> df.printSchema
   * root
   *  |-- target: string (nullable = true)
   *  |-- RA: float (nullable = true)
   *  |-- Dec: float (nullable = true)
   *  |-- Redshift: float (nullable = true)
   *
   * }}}
   */
  // implicit class FitsContext(spark : SparkSession) extends Serializable {
  implicit class FitsContext(spark : SparkSession) extends Serializable {

    // This will contain all options use to load the data
    private[sparkfits] val extraOptions = new scala.collection.mutable.HashMap[String, String]

    // This will contain the info about the schema
    // By default, the schema is inferred from the HDU header,
    // but the user can also manually specify the schema.
    private[sparkfits] var userSpecifiedSchema: Option[StructType] = None

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

    /**
      * Adds a schema to our data. It will overwrite the inferred schema from
      * the HDU header. Useful if the header is corrupted.
      *
      * @param schema : (StructType)
      *   The schema for the data (`StructType(List(StructField))`)
      * @return return the FitsContext (to chain operations)
      */
    def schema(schema: StructType): FitsContext = {
      FitsContext.this.userSpecifiedSchema = Option(schema)
      FitsContext.this
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
      // val nBlock = 100
      // val nParts = 100

      // Check that you can read the data!
      val dataType = Try {
        extraOptions("datatype")
      }
      dataType match {
        case Success(value) => extraOptions("datatype")
        case Failure(e : NullPointerException) =>
          throw new NullPointerException(e.getMessage)
        case Failure(e : NoSuchElementException) =>
          throw new NoSuchElementException("""
          You did not specify the data type!
          Please choose one of the following:
            spark.readfits.option("datatype", "table")
            spark.readfits.option("datatype", "image")
            """)
        case Failure(_) => println("Unknown Exception")
      }

      // Check that the user specifies table
      val dataTypeTable = extraOptions("datatype").contains("table")
      dataTypeTable match {
        case true => extraOptions("datatype")
        case false => throw new AssertionError("""
          Currently only reading data from table is supported.
          Support for image data will be added later.
          Please use spark.readfits.option("datatype", "table")
          """)
      }

      // Check that the user specifies table
      val isIndexHDU = Try {
        extraOptions("HDU")
      }
      isIndexHDU match {
        case Success(value) => extraOptions("HDU")
        case Failure(e : NullPointerException) => throw new NullPointerException(e.getMessage)
        case Failure(e : NoSuchElementException) => throw new NoSuchElementException("""
          You need to specify the HDU to be read!
          spark.readfits.option("HDU", <Int>)
            """)
        case Failure(_) => println("Unknown Exception")
      }

      // Open the file
      val f = new Fits(fn)

      // Grab the desired HDU number
      val indexHDU = extraOptions("HDU").toInt

      // Check that the user specifies table
      val numberOfHdus = getNHdus(f)
      val isHDUBelowMax = indexHDU < numberOfHdus
      isHDUBelowMax match {
        case true => isHDUBelowMax
        case false => throw new AssertionError(s"""
          HDU number $indexHDU does not exist!
          """)
      }

      // Access the meta data
      val hdu = f.getHDU(indexHDU)
      // Check you have indeed a table...
      val dataHdu = Try {
        hdu.asInstanceOf[BinaryTableHDU]
      }
      dataHdu match {
        case Success(value) => value
        case Failure(e : NullPointerException) => throw new NullPointerException(e.getMessage)
        case Failure(e : ClassCastException) => throw new ClassCastException("""
          Data cannot be cast to nom.tam.fits.BinaryTableHDU!
          Are you really trying to access a table as you declared in the option?
          """)
        case Failure(_) => println("Unknown Exception")
      }
      val data = f.getHDU(indexHDU).asInstanceOf[BinaryTableHDU]

      // Get number of rows
      // WARNING: Need to modify nrows, because it could easily by > maxint = 2^31-1...
      // Need also to open a PR to modify getRow to get Long... Moreover
      // I would be able to simplify the stupid nom.tam structure... ;-)
      val nrows : Int = data.getNRows
      val ncols : Int = data.getNCols

      // OMG! fileSize is easily bigger than an Int...
      // Need to develop a test for that... Or put a warning?
      val fileSize : Long = ncols.toLong * nrows.toLong * 8
      // println(fileSize)

      // Assume one block has size 128 Mo
      // If total file size < 128 Mo, divide in 4 blocks.
      // Put an option for the number of blocks!
      val isZero = fileSize < (128 * 1024 * 1024)
      val nBlock : Long = if (!isZero) {
        fileSize / (128 * 1024 * 1024)
      } else 4 // random number...
      // println("################# " + nBlock.toString)

      val sizeBlock : Int = (nrows / nBlock).toInt

      // Get the schema. By default it is built from the header, but the user
      // can also specify it manually.
      val schema = userSpecifiedSchema.getOrElse(getSchema(data))

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
      val rdd = spark.sparkContext.parallelize(0 to nBlock.toInt - 1, nBlock.toInt)
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
    private[sparkfits] def yieldRows(x : Fits,
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
