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
  * Class to handle the relationship between (driver/executors) & HDFS.
  * The idea is to describe the split of the FITS file in block in HDFS.
  * The data is first read in chunks of binary data, then converted to the correct
  * type element by element, and finally grouped into rows.
  *
  * TODO: move the conversion step outside this class (because has no access
  * FitsBlock). However one needs to know where the block starts and stops...
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
    * Core functionality. Here you initialize the data and its split, namely:
    * the data file, the starting index of a block (byte index),
    * the size of one record of data (byte), the ending index of a block (byte).
    * Note that a block can be bigger than a record from the file.
    *
    * @param inputSplit : (InputSplit)
    *   ??
    * @param context : (TaskAttemptContext) currently active context to
    *   access contextual information about running tasks.
    *
    */
  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {
    // the file input
    val fileSplit = inputSplit.asInstanceOf[FileSplit]

    // the actual file we will be reading from
    val file = fileSplit.getPath

    // job configuration
    val conf = context.getConfiguration

    // Initialise our block (header + data)
    fB = new FitsBlock(file, conf, conf.get("HDU").toInt)

    // Define the bytes indices of our block
    startstop = fB.BlockBoundaries
    println("startstop: ", startstop)

    // the byte position this fileSplit starts at
    // Add the offset of the block
    splitStart = fileSplit.getStart + startstop._2
    splitStart = if (splitStart > startstop._3) {
      startstop._3
    } else splitStart

    // Get the header
    header = fB.readHeader

    // Get the number of rows and the size (B) of one row.
    nrowsLong = fB.getNRows(header)
    rowSizeLong = fB.getSizeRowBytes(header)

    // splitEnd byte marker that the fileSplit ends at
    splitEnd = if (splitStart + fileSplit.getLength > startstop._3) {
      startstop._3
    } else splitStart + fileSplit.getLength

    println(s"BLOCK: Start: $splitStart, Stop: $splitEnd")

    // Get the record length in Bytes (get integer!). First look if the user
    // specify a size for the recordLength. If not, set it to 1MB.
    val recordLengthFromUser = Try{conf.get("recordLength").toInt}
      .getOrElse((1 * 1024 * 1024 / rowSizeLong.toInt) * rowSizeLong.toInt)

    // Seek for a round number of lines
    recordLength = (recordLengthFromUser / rowSizeLong.toInt) * rowSizeLong.toInt


    // recordLength = if ((recordLengthFromUser / rowSizeLong.toInt) < nrowsLong.toInt) {
    //   (recordLengthFromUser / rowSizeLong.toInt) * rowSizeLong.toInt
    // } else {
    //   nrowsLong.toInt * rowSizeLong.toInt
    // }

    // Make sure that the recordLength is not bigger than the block size!
    // This is a guard for small files
    recordLength = if ((recordLength / rowSizeLong.toInt)  < nrowsLong.toInt) {
      // OK less than the total number of lines
      recordLength
    } else {
      // Small files, one record is the entire file.
      nrowsLong.toInt * rowSizeLong.toInt
    }

    println(s"recordLength: $recordLength")

    // Move to the starting binary index
    // This should be the getStart of the system offseted by the start
    // of the data block.
    fB.data.seek(splitStart)

    // set our starting block position
    // This should be the getStart of the system offseted by the start
    // of the data block.
    currentPosition = splitStart
  }

  /**
    * Core functionality. Here you describe the relationship between the
    * executors and HDFS. Schematically, when an executor asks to HDFS what to
    * do, the executor executes nextKeyValue.
    *
    * @return (Boolean) true if the executor did not reach the end of the block.
    * false otherwise.
    *
    * TODO: test a while loop instead of the current if. Moreover the boundaries
    * are not correctly handled.
    *
    */
  override def nextKeyValue() : Boolean = {

    // Close the file if start is above end!
    if (splitStart > splitEnd) {
      fB.data.close()
      return false
    }

    // Close the file if we went outside the block!
    if (fB.data.getPos > startstop._3) {
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

    // If recordLength goes above the end of the data block
    recordLength = if ((startstop._3 - fB.data.getPos) < recordLength.toLong) {
        (startstop._3 - fB.data.getPos).toInt
    } else {
        recordLength
    }

    // If recordLength goes above splitEnd
    recordLength = if ((splitEnd - currentPosition) < recordLength.toLong) {
        (splitEnd - currentPosition).toInt
    } else {
        recordLength
    }

    // the recordValue to place the binary data into
    // if (recordValue == null) {
    //     recordValueBytes = new Array[Byte](recordLength)
    // }
    recordValueBytes = new Array[Byte](recordLength)

    // read a record if the currentPosition is less than the split end
    if (currentPosition < splitEnd) {
      // Read a record of length `0 to recordLength - 1`
      fB.data.readFully(recordValueBytes, 0, recordLength)
      println("pos: ", fB.data.getPos)

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
      // println("position (after):" + fB.data.getPos)

      // return true
      return true
    }
    // println(s"EndPosition: $currentPosition (splitEnd: $splitEnd)")
    // println("+-------------------------------------+")
    false
  }
}
