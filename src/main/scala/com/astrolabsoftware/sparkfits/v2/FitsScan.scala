package com.astrolabsoftware.sparkfits.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FitsScan(
                name: String,
                sparkSession: SparkSession,
                options: CaseInsensitiveStringMap,
                schema: StructType
              ) extends Scan with Batch {

  override def toBatch: Batch = super.toBatch

  override def readSchema(): StructType = ???

  override def planInputPartitions(): Array[InputPartition] = Array.empty

  override def createReaderFactory(): PartitionReaderFactory =
    new FitsPartitionReaderFactory(name, sparkSession, options, schema)
}
