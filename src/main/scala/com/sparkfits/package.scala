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

// Low level import
import scala.util.{Try, Success, Failure}

// Hadoop import
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable

// Spark import
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

// Internal import
import com.sparkfits.FitsSchema._
import com.sparkfits.FitsLib.FitsBlock
import com.sparkfits.FitsFileInputFormat._

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
  implicit class FitsContext(spark : SparkSession) extends Serializable {

    // Initialise Hadoop configuration
    val conf = new Configuration(spark.sparkContext.hadoopConfiguration)

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
      // Update options
      FitsContext.this.extraOptions += (key -> value)

      // Update the conf (redundant?)
      conf.set(key, value)

      // Return FitsContext
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
        case Failure(e : NullPointerException) =>
          throw new NullPointerException(e.getMessage)
        case Failure(e : NoSuchElementException) =>
          throw new NoSuchElementException("""
          You need to specify the HDU to be read!
          spark.readfits.option("HDU", <Int>)
            """)
        case Failure(_) => println("Unknown Exception")
      }

      // Open the file
      val path = new org.apache.hadoop.fs.Path(fn)
      val fs = path.getFileSystem(conf)
      val fB = new FitsBlock(path, conf, conf.get("HDU").toInt)

      // Check the header if needed
      if (extraOptions.contains("printHDUHeader")) {
        if (extraOptions("printHDUHeader").toBoolean) {
          val header = fB.readHeader(fB.BlockBoundaries._1)
          val indexHDU = conf.get("HDU").toInt
          println(s"+------ HEADER (HDU=$indexHDU) ------+")
          header.foreach(println)
          println("+----------------------------+")
        }
      }

      // Get the schema. By default it is built from the header, but the user
      // can also specify it manually.
      val schema = userSpecifiedSchema.getOrElse(getSchema(fB))

      // Distribute the data
      val rdd = spark.sparkContext.newAPIHadoopFile(
        fn,
        classOf[FitsFileInputFormat],
        classOf[LongWritable],
        classOf[List[List[_]]],
        conf)
          .flatMap(x => x._2)
          .map(x => Row.fromSeq(x))

      // Return DataFrame with Schema
      spark.createDataFrame(rdd, schema)
    }
  }
}
