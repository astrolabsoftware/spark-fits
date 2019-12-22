package com.astrolabsoftware.sparkfits.v2

import com.astrolabsoftware.sparkfits.FitsLib.Fits
import com.astrolabsoftware.sparkfits.utils.FitsMetadata
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class FitsPartitionReader[T](
                              partition: FilePartition,
                              sparkSession: SparkSession,
                              broadCastedConf: Broadcast[SerializableConfiguration],
                              schema: StructType
                            ) extends PartitionReader[T] {

  // partition will have how many files are in this logical partition. There can be one or more
  // It is ensured that the one file will not be split across multiple partitions, so
  // we don't have to worry about padding, split in middle of row etc etc

  assert(partition.index >= 1, "There are no files in this partition, seems incorrect")

  private val conf = broadCastedConf.value.value
  private var currentFitsMetadata: Option[FitsMetadata] = None
  private var currentFileIndex = 0
  private var fits: Fits = _
  private var recordValueBytes: Array[Byte] = null
  private var currentRow: InternalRow = null

  val log = LogManager.getRootLogger

  private def setCurrentFileParams(): Unit = {
    if (!currentFitsMetadata.isDefined || currentFitsMetadata.get.index != currentFileIndex) {
      currentFitsMetadata = Option(new FitsMetadata(partition.files(currentFileIndex), currentFileIndex, conf))
      fits = currentFitsMetadata.get.fits
    }
  }
  override def next(): Boolean = {
    // We are done reading all the files in the partition
    if (currentFileIndex >= partition.index) {
      return false
    }

    setCurrentFileParams()

    // Close the file if mapper is outside the HDU
    if (currentFitsMetadata.get.notValid) {
      fits.data.close()
      // Try next file
      currentFileIndex += 1
    }

    // Close the file if we went outside the block!
    // This means we sent all our records.
    if (fits.data.getPos >= currentFitsMetadata.get.startStop.dataStop) {
      fits.data.close()
      // Done reading this file, try with the next file in this block
      currentFileIndex += 1
      return next()
    }

    recordValueBytes = new Array[Byte](currentFitsMetadata.get.rowSizeInt)
    fits.data.readFully(recordValueBytes, 0, currentFitsMetadata.get.rowSizeInt)
    currentRow = InternalRow.fromSeq(recordValueBytes)
    true
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    if (fits.data.getPos >= currentFitsMetadata.get.startStop.dataStop) {
      fits.data.close()
    }
  }
}
