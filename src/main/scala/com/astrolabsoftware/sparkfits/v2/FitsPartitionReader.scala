package com.astrolabsoftware.sparkfits.v2

import com.astrolabsoftware.sparkfits.FitsLib
import com.astrolabsoftware.sparkfits.FitsLib.Fits
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

class FitsPartitionReader[T] extends PartitionReader[T] {

  // Initialise mutable variables to be used by the executors
  // Handle the HDFS block boundaries
  private var splitStart: Long = 0L
  private var splitEnd: Long = 0L

  // Cursor position when reading the file
  private var currentPosition: Long = 0L

  // Size of the records to read from the file
  private var recordLength: Int = 0

  // Object to manipulate the fits file
  private var fits: Fits = null
  private var header: Array[String] = null
  private var nrowsLong : Long = 0L
  private var rowSizeInt : Int = 0
  private var rowSizeLong : Long = 0L
  private var startstop: FitsLib.FitsBlockBoundaries = FitsLib.FitsBlockBoundaries()
  private var notValid : Boolean = false

  // The (key, value) used to create the RDD
  private var recordKey: LongWritable = null
  private var recordValue: InternalRow = null

  // Intermediate variable to store binary data
  private var recordValueBytes: Array[Byte] = null

  override def next(): Boolean = {
    // Close the file if mapper is outside the HDU
    if (notValid) {
      fits.data.close()
      return false
    }

    // Close the file if we went outside the block!
    // This means we sent all our records.
    if (fits.data.getPos >= startstop.dataStop) {
      fits.data.close()
      return false
    }

    // Initialise the key of the HDFS block
    if (recordKey == null) {
      recordKey = new LongWritable()
    }

    // the key is a linear index of the record, given by the
    // position the record starts divided by the record length
    recordKey.set(currentPosition / recordLength)

    // The last record might not be of the same size as the other.
    // So if recordLength goes above the end of the data block, cut it.

    // If (getPos + recordLength) goes above splitEnd
    recordLength = if ((startstop.dataStop - fits.data.getPos) < recordLength.toLong) {
      (startstop.dataStop - fits.data.getPos).toInt
    } else {
      recordLength
    }

    // If (currentPosition + recordLength) goes above splitEnd
    recordLength = if ((splitEnd - currentPosition) < recordLength.toLong) {
      (splitEnd - currentPosition).toInt
    } else {
      recordLength
    }

    // Last record may not end at the end of a row.
    // If record length is not a multiple of the row size
    // This can only happen if one of the two ifs below have been triggered
    // (by default recordLength is a multiple of the row size).
    recordLength = if (recordLength % rowSizeLong != 0) {

      // Decrement recordLength until we reach the end of the row n-1.
      do {
        recordLength = recordLength - 1
      } while (recordLength % rowSizeLong != 0)

      // Return
      recordLength
    } else recordLength

    // If recordLength is below the size of a row
    // skip and leave this row for the next block
    if (recordLength < rowSizeLong) {
      fits.data.close()
      return false
    }

    // The array to place the binary data into
    recordValueBytes = new Array[Byte](recordLength)

    // read a record if the currentPosition is less than the split end
    if (currentPosition < splitEnd) {
      // Read a record of length `0 to recordLength - 1`
      fits.data.readFully(recordValueBytes, 0, recordLength)

      // Convert each row
      // 1 task: 32 MB @ 2s
      val tmp = Seq.newBuilder[InternalRow]
      for (i <- 0 to recordLength / rowSizeLong.toInt - 1) {
        tmp += InternalRow.fromSeq(fits.getRow(
          recordValueBytes.slice(
            rowSizeInt*i, rowSizeInt*(i+1))))
      }
      recordValue = tmp.result

      // update our current position
      currentPosition = currentPosition + recordLength

      // we did not reach the end of the split, and we need to send more records
      return true
    }

    // We reached the end of the split.
    // We will now go to another split (if more available)
    fits.data.close()
    false
  }

  override def get(): InternalRow = recordValue

  override def close(): Unit = ???

}
