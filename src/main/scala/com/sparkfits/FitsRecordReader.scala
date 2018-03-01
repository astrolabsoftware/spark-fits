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
class FitsRecordReader extends RecordReader[LongWritable, List[List[_]]] {

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
  private var rowSizeLong : Long = 0L
  private var startstop : (Long, Long, Long, Long) = (0L, 0L, 0L, 0L)

  // The (key, value) used to create the RDD
  private var recordKey: LongWritable = null
  private var recordValue: List[List[_]] = null

  // Intermediate variable to store binary data
  private var recordValueBytes: Array[Byte] = null

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
    * @return (List[List[_]]) Value is a list of heterogeneous lists. It will
    *   be converted to List[Row] later.
    */
  override def getCurrentValue: List[List[_]] = {
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

    // Hadoop description of the input file (Path, split, start/stop indices).
    val fileSplit = inputSplit.asInstanceOf[FileSplit]

    // The actual file we will be reading from
    val file = fileSplit.getPath

    // Uncomment this to get ID identifying the InputSplit in the form
    // hdfs://server.domain:8020/path/to/my/file:start+stop
    // println(fileSplit.toString)

    // Hadoop Job configuration
    val conf = context.getConfiguration

    // Initialise our block (header + data)
    fB = new FitsBlock(file, conf, conf.get("HDU").toInt)

    // Define the bytes indices of our block
    // hdu_start=header_start, data_start, data_stop, hdu_stop
    startstop = fB.BlockBoundaries

    // Get the header
    header = fB.readHeader

    // Get the number of rows and the size (B) of one row.
    nrowsLong = fB.getNRows(header)
    rowSizeLong = fB.getSizeRowBytes(header)


    // A priori, there is no reason for a random split of the FITS file to start
    // at the beginning of a row. Therefore we do the following:
    //  - The first block starts at 0 + header_start
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

    // We assume that fileSplit.getStart starts at 0 for the first block.
    val start = if((fileSplit.getStart) % rowSizeLong != 0) {

      // Decrement the starting index to fully catch the line we are sitting on
      var tmp_byte = 0
      do {
        tmp_byte = tmp_byte - 1
      } while ((fileSplit.getStart + tmp_byte) % rowSizeLong != 0)

      // Return offseted starting index
      fileSplit.getStart + tmp_byte
    } else fileSplit.getStart

    // the byte position this fileSplit starts at
    // Add the header offset to the starting position block
    splitStart = start + startstop._2

    // If the splitStart is above the end of the FITS HDU, reduce it.
    // Concretely, that means there is nothing else to do, and nextKeyValue
    // will return False. This is completely an artifact of the way we
    // distribute FITS -> the number of blocks is determined with the size
    // of the file, but we are interested in only one HDU inside this file.
    // Therefore, there will be blocks not containing data from this HDU, and
    // their starting index will be above the end of the HDU.
    // This is clearly a waste of resource, and not efficient at all.
    // TODO: Extend InputSplit.
    splitStart = if (splitStart > startstop._3) {
      startstop._3
    } else splitStart

    // splitEnd byte marker that the fileSplit ends at
    // Truncate the splitEnd if it goes above the end of the HDU
    splitEnd = if (splitStart + fileSplit.getLength > startstop._3) {
      startstop._3
    } else splitStart + fileSplit.getLength

    // println(s"BLOCK: Start: $splitStart, Stop: $splitEnd")

    // Get the record length in Bytes (get integer!). First look if the user
    // specify a size for the recordLength. If not, set it to 128 Ko.
    val recordLengthFromUser = Try{conf.get("recordLength").toInt}
      .getOrElse((128 * 1024 / rowSizeLong.toInt) * rowSizeLong.toInt)

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

    // Close the file if splitStart is above splitEnd!
    // See initialize for this pathological case.
    if (splitStart >= splitEnd) {
      fB.data.close()
      return false
    }

    // Close the file if we went outside the block!
    // This means we sent all our records.
    if (fB.data.getPos >= startstop._3) {
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
    recordLength = if ((startstop._3 - fB.data.getPos) < recordLength.toLong) {
        (startstop._3 - fB.data.getPos).toInt
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

      // Group by row
      val it = recordValueBytes.grouped(rowSizeLong.toInt)

      // Convert each row
      val tmp = for {
        i <- 0 to recordLength / rowSizeLong.toInt - 1
      } yield (fB.readLineFromBuffer(it.next()))

      // Back to List
      // recordValue = tmp.map(x=>Row.fromSeq(x)).toList
      recordValue = tmp.toList

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
