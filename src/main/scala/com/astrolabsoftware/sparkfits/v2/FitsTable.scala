package com.astrolabsoftware.sparkfits.v2

import com.astrolabsoftware.sparkfits.FitsLib.Fits
import com.astrolabsoftware.sparkfits.FitsSchema.getSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class FitsTable(
                      name: String,
                      sparkSession: SparkSession,
                      options: CaseInsensitiveStringMap,
                      paths: Seq[String],
                      userSpecifiedSchema: Option[StructType],
                      fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new FitsScanBuilder(name, sparkSession, options, schema)

  override lazy val schema: StructType = userSpecifiedSchema.getOrElse {
    val conf = new Configuration(sparkSession.sparkContext.hadoopConfiguration)
    val pathFS = new Path(paths(0))
    val fits = new Fits(pathFS, conf, options.get("hdu").toInt)
    // Register header and block boundaries
    // in the Hadoop configuration for later re-use
    fits.registerHeader
    fits.blockBoundaries.register(pathFS, conf)
    getSchema(fits)
  }

  override def formatName: String = "FITS"

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = ???

  //  override def name(): String = ???
  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = ???
}