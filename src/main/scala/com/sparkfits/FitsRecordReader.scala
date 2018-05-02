/*
 * Copyright 2018 Julien Peloton
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
package com.sparkfits

// Java dependencies
import java.io.IOException
import java.nio.ByteBuffer

// Scala dependencies
import scala.util.{Try, Success, Failure}
import scala.collection.mutable

// Hadoop dependencies
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit

// Spark dependencies
import org.apache.spark.sql.Row

// Internal dependencies
import com.sparkfits.FitsLib.FitsBlock

/**
  * Class to handle the relationship between executors & HDFS when reading a
  * FITS file:
  *   File -> InputSplit -> RecordReader (this class) -> Mapper (executors)
  * It extends the abstract class RecordReader from Hadoop.
  * The idea behind is to describe the split of the FITS file
  * in block and splits in HDFS. First the file is split into blocks
  * in HDFS (physical blocks), whose size are given by Hadoop configuration
  * (typically 128 MB). Then inside a block, the data is sent to executors
  * record-by-record (logical split) of size < 128 MB.
  * The purpose of this class is to describe the 2nd step, that is the split
  * of blocks in records.
  *
  * The data is first read in chunks of binary data, then converted to the correct
  * type element by element, and finally grouped into rows.
  *
  */
class FitsRecordReader extends RecordReader[LongWritable, Seq[Row]] {

  // Initialise mutable variables to be used by the executors
  // Handle the HDFS block boundaries
  private var splitStart: Long = 0L
  private var splitEnd: Long = 0L

  // Cursor position when reading the file
  private var currentPosition: Long = 0L

  // Size of the records to read from the file
  private var recordLength: Int = 0

  // Object to manipulate the fits file
  private var fB: FitsBlock = null
  private var header: Array[String] = null
  private var nrowsLong : Long = 0L
  private var rowSizeInt : Int = 0
  private var rowSizeLong : Long = 0L
  private var startstop: FitsLib.FitsBlockBoundaries = FitsLib.FitsBlockBoundaries()
  private var notValid : Boolean = false

  // The (key, value) used to create the RDD
  private var recordKey: LongWritable = null
  private var recordValue: Seq[Row] = null

  // Intermediate variable to store binary data
  private var recordValueBytes: Array[Byte] = null
  // private var recordValueBytes: ByteBuffer = null

  /**
    * Close the file after reading it.
    */
  override def close() {
    if (fB.data != null) {
      fB.data.close()
    }
  }

  /**
    * Get the current Key.
    * @return (LongWritable) key.
    */
  override def getCurrentKey: LongWritable = {
    recordKey
  }

  /**
    * Get the current Value.
    * @return (Seq[Row]) Value is a list of heterogeneous lists. It will
    *   be converted to List[Row] later.
    */
  override def getCurrentValue: Seq[Row] = {
    recordValue
  }

  /**
    * Fancy way of getting a process bar. Useful to know whether you have
    * time for a coffee and a cigarette before the next run.
    *
    * @return (Float) progression inside a block.
    */
  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0.toFloat
      case _ => Math.min(
        ((currentPosition - splitStart) / (splitEnd - splitStart)).toFloat, 1.0
      ).toFloat
    }
  }

  /**
    * Here an executor will come and ask for a block of data
    * by calling initialize(). Hadoop will split the data into records and
    * those records will be sent. One needs then to know: the data file,
    * the starting index of a split (byte index), the size of one record of
    * data (byte), the ending index of a split (byte).
    *
    * Typically, a record must not be bigger than 1MB for the process to be
    * efficient. Otherwise you will have a lot of Garbage collector call!
    *
    * @param inputSplit : (InputSplit)
    *   Represents the data to be processed by an individual Mapper.
    * @param context : (TaskAttemptContext)
    *   Currently active context to access contextual information about
    *   running tasks.
    * @return (Long) the current position of the pointer cursor in the file.
    *
    */
  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {

    // println(s"FitsRecordReader.initialize> ")
    // Hadoop description of the input file (Path, split, start/stop indices).
    val fileSplit = inputSplit.asInstanceOf[FileSplit]

    // The actual file we will be reading from
    val file = fileSplit.getPath

    // Uncomment this to get ID identifying the InputSplit in the form
    // hdfs://server.domain:8020/path/to/my/file:start+length
    // println(fileSplit.toString)

    // Hadoop Job configuration
    val conf = context.getConfiguration

    // Initialise our block (header + data)
    fB = new FitsBlock(file, conf, conf.get("hdu").toInt)

    // Define the bytes indices of our block
    // hdu_start=header_start, dataStart, dataStop, hdu_stop
    startstop = fB.blockBoundaries

    // Get the header
    header = fB.blockHeader
    val keyValues = FitsLib.parseHeader(header)

    // Get the number of rows and the size (B) of one row.
    // this is dependent on the HDU type
    nrowsLong = fB.hdu.getNRows(keyValues)
    rowSizeInt = fB.hdu.getSizeRowBytes(keyValues)
    rowSizeLong = rowSizeInt.toLong

    // println(s"FitsRecordReader.initialize> nrowsLong=$nrowsLong rowSizeInt=$rowSizeInt")

    // What Hadoop gave us
    val start_theo = fileSplit.getStart
    val stop_theo = fileSplit.getStart + fileSplit.getLength

    // Reject this mapper if the HDFS block is below the targeted HDU
    notValid = if((start_theo < startstop.dataStart) && (stop_theo < startstop.dataStart)) {
      true
    } else if ((start_theo >= startstop.dataStop) && (stop_theo >= startstop.dataStop)) {
      true
    } else {
      false
    }

    val splitStart_tmp = if(start_theo <= startstop.dataStart && !notValid) {
      // Valid block: starting index.
      // We are just before the targeted HDU, therefore
      // we jump at the beginning of the data block
      startstop.dataStart
    } else {
      start_theo
    }

    splitEnd = if(stop_theo <= startstop.dataStop && !notValid) {
      // Valid block: ending index (start/end inside)
      // We are inside the targeted HDU
      stop_theo
    } else if (stop_theo > startstop.dataStop && !notValid) {
      // Valid block: ending index (start inside, end outside)
      // The block start in the targeted HDU, but ends outside.
      // We just move back the final cursor.
      startstop.dataStop
    } else {
      // Not valid anyway
      stop_theo
    }

    // A priori, there is no reason for a random split of the FITS file to start
    // at the beginning of a row. Therefore we do the following:
    //  - The block starts
    //  - its data is processed record-by-record (see below for the
    //    processing of the records)
    //  - at the end of the block, the stop index might be in the middle of a
    //    row. We do not read this row in the first block, and we stop here.
    //  - The second block starts at start_1=(end_0)
    //  - We decrement the starting index to include the previous line not read
    //    in the first block.
    //  - its data is processed record-by-record
    //  - etc.
    // Summary: Add last row if we start the block at the middle of a row.
    // We assume that fileSplit.getStart starts at the
    // beginning of the data block for the first valid block.
    splitStart = if((splitStart_tmp) % rowSizeLong != startstop.dataStart &&
      splitStart_tmp != startstop.dataStart && splitStart_tmp != 0) {

      // Decrement the starting index to fully catch the line we are sitting on
      var tmp_byte = 0
      do {
        tmp_byte = tmp_byte - 1
      } while ((splitStart_tmp + tmp_byte) % rowSizeLong != 0)

      // Return offseted starting index
      splitStart_tmp + tmp_byte
    } else splitStart_tmp

    // println(s"BLOCK: Start: $splitStart, Stop: $splitEnd")

    // Get the record length in Bytes (get integer!). First look if the user
    // specify a size for the recordLength. If not, set it to 1 Ko.
    val recordLengthFromUser = Try{conf.get("recordlength").toInt}
      .getOrElse((1 * 1024 / rowSizeLong.toInt) * rowSizeLong.toInt)

    // Seek for a round number of lines for the record
    recordLength = (recordLengthFromUser / rowSizeLong.toInt) * rowSizeLong.toInt

    // Make sure that the recordLength is not bigger than the block size!
    // This is a guard for small files.
    recordLength = if ((recordLength / rowSizeLong.toInt)  < nrowsLong.toInt) {
      // OK less than the total number of lines
      recordLength
    } else {
      // Small files, one record is the entire file.
      nrowsLong.toInt * rowSizeLong.toInt
    }

    // Move to the starting binary index
    fB.data.seek(splitStart)

    // Set our starting block position
    currentPosition = splitStart
  }

  /**
    * Here you describe how the records are made, and the split data sent.
    *
    * @return (Boolean) true if the Mapper did not reach the end of the split.
    * false otherwise.
    *
    */
  override def nextKeyValue() : Boolean = {

    // Close the file if mapper is outside the HDU
    if (notValid) {
      fB.data.close()
      return false
    }

    // Close the file if we went outside the block!
    // This means we sent all our records.
    if (fB.data.getPos >= startstop.dataStop) {
      fB.data.close()
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
    recordLength = if ((startstop.dataStop - fB.data.getPos) < recordLength.toLong) {
        (startstop.dataStop - fB.data.getPos).toInt
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
      fB.data.close()
      return false
    }

    // The array to place the binary data into
    recordValueBytes = new Array[Byte](recordLength)

    // read a record if the currentPosition is less than the split end
    if (currentPosition < splitEnd) {
      // Read a record of length `0 to recordLength - 1`
      fB.data.readFully(recordValueBytes, 0, recordLength)

      // Convert each row
      // 1 task: 32 MB @ 2s
      val tmp = Seq.newBuilder[Row]
      for (i <- 0 to recordLength / rowSizeLong.toInt - 1) {
        tmp += Row.fromSeq(fB.readLineFromBuffer(
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
    false
  }
}
