package com.astrolabsoftware.sparkfits.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.conf.Configuration

class FitsScanBuilder(
                       sparkSession: SparkSession,
                       conf: Configuration,
                       schema: StructType
                     ) extends ScanBuilder {

  override def build(): Scan = new FitsScan(sparkSession, conf, schema)

}
