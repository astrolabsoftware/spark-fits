package com.astrolabsoftware.sparkfits.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.execution.datasources.FilePartition

class FitsPartitionReaderFactory(
                                  sparkSession: SparkSession,
                                  conf: Configuration,
                                  schema: StructType
                                ) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    new FitsPartitionReader[InternalRow](filePartition, sparkSession, conf, schema)
  }
}
