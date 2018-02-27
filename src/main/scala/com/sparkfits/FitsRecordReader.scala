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

    // the byte position this fileSplit starts at
    splitStart = fileSplit.getStart

    // the actual file we will be reading from
    val file = fileSplit.getPath

    // job configuration
    val conf = context.getConfiguration

    // Initialise our block (header + data)
    fB = new FitsBlock(file, conf, conf.get("HDU").toInt)

    // Define the bytes indices of our block
    val startstop = fB.BlockBoundaries

    // Get the header
    header = fB.readHeader

    // Get the number of rows and the size (B) of one row.
    nrowsLong = fB.getNRows(header)
    rowSizeLong = fB.getSizeRowBytes(header)

    // splitEnd byte marker that the fileSplit ends at
    splitEnd = if (nrowsLong * rowSizeLong < splitStart + fileSplit.getLength) {
      nrowsLong * rowSizeLong
    } else splitStart + fileSplit.getLength

    // Get the record length in Bytes (get integer!). First look if the user
    // specify a size for the recordLength. If not, set it to 1MB.
    val recordLengthFromUser = Try{conf.get("recordLength").toInt}
      .getOrElse((1 * 1024 * 1024 / rowSizeLong.toInt) * rowSizeLong.toInt)

    // Make sure that the recordLength is not bigger than the block size!
    recordLength = if ((recordLengthFromUser / rowSizeLong.toInt) < nrowsLong.toInt) {
      recordLengthFromUser
    } else {
      nrowsLong.toInt * rowSizeLong.toInt
    }

    // set our starting block position
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
    if (recordKey == null) {
      recordKey = new LongWritable()
    }
    // the key is a linear index of the record, given by the
    // position the record starts divided by the record length
    recordKey.set(currentPosition / recordLength)

    // the recordValue to place the binary data into
    if (recordValue == null) {
      recordValueBytes = new Array[Byte](recordLength)
    }
    // read a record if the currentPosition is less than the split end
    if (currentPosition < splitEnd) {

      // Read a record of length `0 to recordLength - 1`
      fB.data.read(recordValueBytes, 0, recordLength)

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

      // return true
      return true
    }
    // println(s"Start: $splitStart EndPosition : " + currentPosition.toString)
    false
  }
}
