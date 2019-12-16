package com.astrolabsoftware.sparkfits.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FitsScanBuilder(
                       name: String,
                       sparkSession: SparkSession,
                       options: CaseInsensitiveStringMap,
                       schema: StructType
                     ) extends ScanBuilder {
  override def build(): Scan = new FitsScan(name, sparkSession, options, schema)


}
