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
import java.io.EOFException
import java.io.InputStream
import org.apache.hadoop.fs.FSDataInputStream
import scala.util.{Try, Success, Failure}
import scala.collection.mutable.HashMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import org.apache.spark.SparkContext

import nom.tam.fits.{Fits, HeaderCard, Header, BinaryTableHDU}
import nom.tam.util.{Cursor, AsciiFuncs}



/**
  * Object to manipulate metadata of the fits file.
  */
object SparkFitsUtil {

  /**
    * Get the number of HDUs in the fits file.
    * This method should be used recursively.
    *
    * @param f : (nom.tam.fits.Fits)
    *   The fits file opened with nom.tam.fits.Fits
    * @param n : (Int)
    *   The index of the current HDU.
    * @return the number of HDU in the fits file.
    *
    */
  def getNHdus(f : Fits, n : Int = 0) : Int = {
    if (f.getHDU(n) != null) getNHdus(f, n + 1) else n
  }

  /**
    * Get the header of a HDU recursively. The method returns a String with all
    * the header. The different elements of the original header are separated by
    * a coma. To access them individually, use the split(",") method on the final
    * string.
    *
    * @param c : (nom.tam.util.Cursor)
    *   Cursor to navigate in the header of the HDU. Work as an Iterator.
    * @param s : (String)
    *   The string that will contain the Header.
    * @return A string containing the Header, whose elements are coma-separated.
    *
    */
  def getMyHeader(c : Cursor[String, HeaderCard], s : String) : String = {
    if (c.hasNext() == true) getMyHeader(c, s + c.next() + ",") else s
  }

  /**
    * Return the number of Rows as Long (as opposed to the method getNRows
    * which returns an Int).
    *
    * @param hdu : (BinaryTableHDU)
    *   The HDU containing the header and the table data.
    * @return The number of rows as a Long.
    */
  def getNRowsFromHeader(hdu : BinaryTableHDU) : Long = {
    val nRowsLong = hdu.getHeader.getLongValue("NAXIS2")
    nRowsLong
  }

  /**
    * Check if the user is trying to read data from HDFS. Can be better...
    * TODO: Check when executing that file format match requirements...
    *
    * @param fn : (String)
    *   Filename. If local[*], you must specify the full path to the fits file
    *   If cluster mode (standalone or mesos), you must specify the path to
    *   the directory containing parquet files in hdfs.
    * @return Boolean. True if the file is local, otherwise false.
    */
  def checkIsHdfsFile(fn : String) : Boolean = {
    // check if file starts by hdfs:
    fn.startsWith("hdfs:")
  }

  /**
    * Check if the we are running a job in local or in cluster mode
    *
    * @param sc : (SparkContext)
    *   The spark context of the job.
    * @return Boolean. True if we are in spark standalone mode, otherwise false.
    */
  def checkIsClusterMode(sc : SparkContext) : Boolean = {
    // check if file starts by hdfs:
    sc.master.startsWith("spark:")
  }

  def hdfsDirExists(hdfsDirectory: String) : Boolean = {
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)
    val exists = fs.exists(new Path(hdfsDirectory))
    return exists
  }
}
