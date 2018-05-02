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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.fs.RemoteIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.LocatedFileStatus

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.sources.BaseRelation

import com.sparkfits.FitsLib.FitsBlock
import com.sparkfits.FitsSchema.getSchema
import com.sparkfits.FitsFileInputFormat._

/**
  * Data Source API implementation for FITS.
  * Note that for the moment, we provide support only for FITS table.
  * We will add FITS image later on.
  *
  * The interpreter session below shows how to use basic functionalities:
  *
  * {{{
  * scala> val fn = "src/test/resources/test_file.fits"
  * scala> val df = spark.read
  *  .format("com.sparkfits")
  *  .option("hdu", 1)
  *  .option("verbose", true)
  *  .load(fn)
  * +------ HEADER (HDU=1) ------+
  * XTENSION= BINTABLE           / binary table extension
  * BITPIX  =                    8 / array data type
  * NAXIS   =                    2 / number of array dimensions
  * NAXIS1  =                   34 / length of dimension 1
  * NAXIS2  =                20000 / length of dimension 2
  * PCOUNT  =                    0 / number of group parameters
  * GCOUNT  =                    1 / number of groups
  * TFIELDS =                    5 / number of table fields
  * TTYPE1  = target
  * TFORM1  = 10A
  * TTYPE2  = RA
  * TFORM2  = E
  * TTYPE3  = Dec
  * TFORM3  = D
  * TTYPE4  = Index
  * TFORM4  = K
  * TTYPE5  = RunId
  * TFORM5  = J
  * END
  * +----------------------------+
  * df: org.apache.spark.sql.DataFrame = [target: string, RA: float ... 3 more fields]
  *
  * scala> df.printSchema
  * root
  *  |-- target: string (nullable = true)
  *  |-- RA: float (nullable = true)
  *  |-- Dec: double (nullable = true)
  *  |-- Index: long (nullable = true)
  *  |-- RunId: integer (nullable = true)
  *
  * scala> df.show(5)
  * +----------+---------+--------------------+-----+-----+
  * |    target|       RA|                 Dec|Index|RunId|
  * +----------+---------+--------------------+-----+-----+
  * |NGC0000000| 3.448297| -0.3387486324784641|    0|    1|
  * |NGC0000001| 4.493667| -1.4414990980543227|    1|    1|
  * |NGC0000002| 3.787274|  1.3298379564211742|    2|    1|
  * |NGC0000003| 3.423602|-0.29457151504987844|    3|    1|
  * |NGC0000004|2.6619017|  1.3957536426732444|    4|    1|
  * +----------+---------+--------------------+-----+-----+
  * only showing top 5 rows
  *
  * }}}
  */
class FitsRelation(parameters: Map[String, String], userSchema: Option[StructType])(@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan {

  // override def toString: String = "FITS"
  //
  // override def hashCode(): Int = getClass.hashCode()
  //
  // override def equals(other: Any): Boolean = other.isInstanceOf[FitsFileInputFormat]

  // Level of verbosity
  var verbosity : Boolean = false

  // Initialise Hadoop configuration
  val conf = new Configuration(sqlContext.sparkContext.hadoopConfiguration)

  // This will contain all options use to load the data
  private[sparkfits] val extraOptions = new scala.collection.mutable.HashMap[String, String]

  // Pre-load basic parameters for quick checks
  val filePath = parameters.get("path") match {
    case Some(x) => x
    case None => sys.error("'path' must be specified.")
  }

  val indexHDU = parameters.get("hdu") match {
    case Some(x) => x
    case None => throw new NoSuchElementException("""
    You need to specify the HDU to be read!
    spark.readfits.option("hdu", <Int>)
      """)
  }

  /**
    * Search for input FITS files. The input path can be either a single
    * FITS file, or a folder containing several FITS files with the
    * same HDU structure. Raise a NullPointerException if no files found.
    *
    * @param fn : (String)
    *   Input path.
    * @return (List[String]) List with all files found.
    *
    */
  def searchFitsFile(fn: String): List[String] = {
    // Make it Hadoop readable
    val path = new Path(fn)
    val fs = path.getFileSystem(conf)

    // Check whether we want to load a single FITS file or several
    val isDir = fs.isDirectory(path)
    val isFile = fs.isFile(path)

    // println(s"isDir=$isDir isFile=$isFile path=$path")

    // List all the files
    val listOfFitsFiles : List[String] = if (isDir) {
      val it = fs.listFiles(path, true)
      getListOfFiles(it).filter{file => file.endsWith(".fits")}
    } else if (isFile){
      List(fn)
    } else {
      List[String]()
    }

    // Check that we have at least one file
    listOfFitsFiles.size match {
      case x if x > 0 => if (verbosity) {
        println("FitsRelation.searchFitsFile> Found " + listOfFitsFiles.size.toString + " file(s):")
        listOfFitsFiles.foreach(println)
      }
      case x if x <= 0 => throw new NullPointerException(s"""
          0 files detected! Is $fn a directory containing
          FITS files or a FITS file?
          """)
    }

    listOfFitsFiles
  }

  /**
    * Load recursively all FITS file inside a directory.
    *
    * @param it : (RemoteIterator[LocatedFileStatus])
    *   Iterator from a Hadoop Path containing informations about files.
    * @param extensions : (List[String)
    *   List of accepted extensions. Currently only .fits is available.
    *   Default is List("*.fits").
    * @return List of files as a list of String.
    *
    */
  def getListOfFiles(it: RemoteIterator[LocatedFileStatus],
      extensions: List[String] = List(".fits")): List[String] = {
    if (!it.hasNext) {
      Nil
    } else {
      it.next.getPath.toString :: getListOfFiles(it, extensions)
    }
  }

  /**
    * Check that the schemas of different FITS HDU to be added are
    * the same. Throw an AssertionError otherwise.
    * The check is performed only for BINTABLE.
    *
    * @param listOfFitsFiles : (List[String])
    *   List of files as a list of String.
    * @return (String) the type of HDU: BINTABLE, IMAGE, EMPTY, or
    *   NOT UNDERSTOOD if not registered.
    *
    */
  def checkSchemaAndReturnType(listOfFitsFiles : List[String]): Boolean = {
    // Wanted HDU
    val indexHDU = conf.get("hdu").toInt

    // Initialise
    val path_init = new Path(listOfFitsFiles(0))

    val fB_init = new FitsBlock(path_init, conf, indexHDU)

    if (fB_init.hdu.implemented) {
      val schema_init = getSchema(fB_init)
      fB_init.data.close()

      for (file <- listOfFitsFiles.slice(1, listOfFitsFiles.size)) {
        var path = new Path(file)
        val fB = new FitsBlock(path, conf, indexHDU)
        val schema = getSchema(fB)
        val isOk = schema_init == schema
        isOk match {
          case true => isOk
          case false => {
            // println(listOfFitsFiles(0))
            // println("----> ", schema_init)
            // println(file)
            // println("----> ", schema)
            throw new AssertionError(
              """
            You are trying to add HDU data with different structures!
            Check that the number of columns, names of columns and element
            types are the same. re-run with .option("verbose", true) to
            list the files.
          """)
          }
        }
        fB.data.close()
      }
      true
    } else {
      println(s"""
        FITS type ${fB_init.hduType} not supported yet.
        An empty DataFrame will be returned.""")
      false
    }
  }

  /**
    * Create a RDD[Row] from the data of one HDU.
    * The input can be either the path to one FITS file (path + filename),
    * or the path to a directory containing FITS files. In the latter,
    * the code will load all FITS files listed inside this directory
    * and make the union of the HDU data. Needless to say that the FITS
    * files must have the same structure, otherwise the union will be impossible.
    * The format of the input must be a String with Hadoop format
    *   - (local) file://path/to/data
    *   - (HDFS)  hdfs://<IP>:<PORT>//path/to/data
    *
    *
    * If the HDU type is not "implemented", return an empty RDD[Row].
    *
    * @param fn : (String)
    *   Filename of the fits file to be read, or a directory containing FITS files
    *   with the same HDU structure.
    * @return (RDD[Row]) always one single RDD made from the HDU of
    *   one FITS file, or from the same kind of HDU from several FITS file.
    *   Empty if the HDU type is not a BINTABLE.
    *
    */
  def load(fn : String): RDD[Row] = {

    val listOfFitsFiles = searchFitsFile(fn)

    // Check that all the files have the same Schema
    // in order to perform the union. Return the HDU type.
    val implemented = checkSchemaAndReturnType(listOfFitsFiles)

    // Load one or all the FITS files found
    load(listOfFitsFiles, implemented)
  }

  /**
    * Load the HDU data from several FITS file into a single RDD[Row].
    * The structure of the HDU must be the same, that is contain the
    * same number of columns with the same name and element types.
    *
    * If the HDU type is not "implemented", return an empty RDD[Row].
    *
    * @param fns : (List[String])
    *   List of filenames with the same structure.
    * @return (RDD[Row]) always one single RDD[Row] made from the HDU of
    *   one FITS file, or from the same kind of HDU from several FITS file.
    *   Empty if the HDU type is not a BINTABLE.
    *
    */
  def load(fns : List[String], implemented: Boolean): RDD[Row] = {

    // Number of files
    val nFiles = fns.size

    // Initialise
    var rdd = if (implemented) {
      loadOneTable(fns(0))
    } else {
      loadOneEmpty
    }

    // Union if more than one file
    for ((file, index) <- fns.slice(1, nFiles).zipWithIndex) {
      rdd = if (implemented) {
        rdd.union(loadOneTable(file))
      } else {
        rdd.union(loadOneEmpty)
      }
    }
    rdd
  }

  /** Load a xxx FITS data contained in one HDU as a RDD[Row].
    *
    * @param fn : (String)
    *   Path + filename of the fits file to be read.
    * @return : RDD[Row] made from one single HDU.
    */
  def loadOneTable(fn : String): RDD[Row] = {

    // Open one file
    val path = new Path(fn)
    val indexHDU = conf.get("hdu").toInt
    val fB = new FitsBlock(path, conf, indexHDU)

    // Register header and block boundaries in the Hadoop configuration
    fB.registerHeader()
    fB.blockBoundaries.register(path, conf)

    // Check the header if needed
    if (verbosity) {
      println(s"+------ FILE $fn ------+")
      println(s"+------ HEADER (HDU=$indexHDU) ------+")
      fB.blockHeader.foreach(println)
      println("+----------------------------+")
    }

    // We do not need the data on the driver at this point.
    // The executors will re-open it later on.
    fB.data.close()

    // Distribute the table data
    sqlContext.sparkContext.newAPIHadoopFile(fn,
      classOf[FitsFileInputFormat],
      classOf[LongWritable],
      classOf[Seq[Row]],
      conf).flatMap(x => x._2)
  }

  /**
    * Return an empty RDD of Row.
    *
    * @return (RDD[Row]) Empty RDD.
    */
  def loadOneEmpty : RDD[Row] = {
    sqlContext.sparkContext.emptyRDD[Row]
  }

  /**
    * Register user parameters in the configuration (broadcasted).
    */
  def registerConfigurations: Unit = {
    for (keyAndVal <- parameters) {
      conf.set(keyAndVal._1, keyAndVal._2)
      extraOptions += (keyAndVal._1 -> keyAndVal._2)
    }
  }

  /**
    * The schema of the DataFrame is inferred from the
    * header of the fits HDU directly unless the user specifies it.
    *
    * @return (StructType) schema for the DataFrame
    */
  override def schema: StructType = {
    registerConfigurations
    userSchema.getOrElse{
      val listOfFitsFiles = searchFitsFile(filePath)

      val pathFS = new Path(listOfFitsFiles(0))
      val fB = new FitsBlock(pathFS, conf, conf.get("hdu").toInt)
      // Register header and block boundaries
      // in the Hadoop configuration for later re-use
      fB.registerHeader()
      fB.blockBoundaries.register(pathFS, conf)
      getSchema(fB)
    }
  }

  /**
    * Create RDD[Row] from FITS HDU data.
    *
    * @return (RDD[Row])
    */
  override def buildScan(): RDD[Row] = {

    // Register the user parameters in the Hadoop conf
    registerConfigurations

    // Level of verbosity. Default is false
    verbosity = Try{extraOptions("verbose")}.getOrElse("false").toBoolean

    // Distribute the data
    load(filePath)
  }
}
