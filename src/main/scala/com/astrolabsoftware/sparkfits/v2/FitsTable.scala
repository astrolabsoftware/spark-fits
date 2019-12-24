/*
 * Copyright 2019 AstroLab Software
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
package com.astrolabsoftware.sparkfits.v2

import com.astrolabsoftware.sparkfits.FitsLib.Fits
import com.astrolabsoftware.sparkfits.FitsSchema.getSchema
import com.astrolabsoftware.sparkfits.utils.FitsUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

case class FitsTable(
                      sparkSession: SparkSession,
                      options: CaseInsensitiveStringMap,
                      userSpecifiedSchema: Option[StructType])
  extends Table with SupportsRead {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new FitsScanBuilder(sparkSession, conf, schema)

  // Initialise Hadoop configuration
  val conf = new Configuration(sparkSession.sparkContext.hadoopConfiguration)

  // This will contain all options use to load the data
  private val extraOptions = new scala.collection.mutable.HashMap[String, String]
  private val optionsAsScala: mutable.Map[String, String] = mutable.Map.empty
  optionsAsScala ++= options.asScala
  private final val listOfFitsFiles = searchFitsFile(optionsAsScala("path"), conf, verbosity)
  // Add list of Fits files for a use later
  optionsAsScala += ("listOfFitsFiles" -> listOfFitsFiles.mkString(","))

  def registerConfigurations: Unit = {
    for (keyAndVal <- optionsAsScala) {
      conf.set(keyAndVal._1, keyAndVal._2)
      extraOptions += (keyAndVal._1 -> keyAndVal._2)
    }
    if (conf.get("mode") == null) {
      conf.set("mode", "PERMISSIVE")
      extraOptions += ("mode" -> "PERMISSIVE")
    }
  }
  registerConfigurations
  val verbosity = Try{extraOptions("verbose")}.getOrElse("false").toBoolean

  override lazy final val schema: StructType = userSpecifiedSchema.getOrElse {

    // Check that all the files have the same Schema
    // in order to perform the union. Return the HDU type.
    // NOTE: This operation is very long for hundreds of files!
    // NOTE: Limit that to the first 10 files.
    // NOTE: Need to be fixed!
    val implemented = if (listOfFitsFiles.size < 10) {
      checkSchemaAndReturnType(listOfFitsFiles, conf)
    } else{
      checkSchemaAndReturnType(listOfFitsFiles.slice(0, 10), conf)
    }
    conf.setBoolean("implemented", implemented)

    val pathFS = new Path(listOfFitsFiles(0))
    val fits = new Fits(pathFS, conf, options.get("hdu").toInt)
    // Register header and block boundaries
    // in the Hadoop configuration for later re-use
    fits.registerHeader
    fits.blockBoundaries.register(pathFS, conf)
    getSchema(fits)
  }

  // We don't really have the notion of table name FITS. So just returning the location
  override def name(): String = s"FITS Table: ${options.get("path")}"

  override def capabilities: java.util.Set[TableCapability] = Set(BATCH_READ).asJava
}
