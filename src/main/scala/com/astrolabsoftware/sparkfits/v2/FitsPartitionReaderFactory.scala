package com.astrolabsoftware.sparkfits.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.util.SerializableConfiguration

class FitsPartitionReaderFactory(
                                  sparkSession: SparkSession,
                                  broadCastedConf: Broadcast[SerializableConfiguration],
                                  schema: StructType
                                ) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    new FitsPartitionReader[InternalRow](filePartition, sparkSession, broadCastedConf, schema)
  }
}
