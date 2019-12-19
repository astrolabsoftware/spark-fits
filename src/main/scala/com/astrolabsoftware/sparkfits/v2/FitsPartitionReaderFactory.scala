package com.astrolabsoftware.sparkfits.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FitsPartitionReaderFactory(
                                  sparkSession: SparkSession,
                                  options: CaseInsensitiveStringMap,
                                  schema: StructType
                                ) extends FilePartitionReaderFactory {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] =
    new FitsPartitionReader[InternalRow]

}
