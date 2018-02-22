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

import nom.tam.fits.{Fits, BinaryTableHDU}
import com.sparkfits.SparkFitsUtil._
import com.sparkfits.FitsBlock._

import java.io.IOException

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{BytesWritable, LongWritable, ObjectWritable}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import org.apache.spark.sql.Row

class FitsRecordReader extends RecordReader[LongWritable, List[Row]] {
  private var splitStart: Long = 0L
  private var splitEnd: Long = 0L
  private var currentPosition: Long = 0L
  private var recordLength: Int = 0
  private var fB: FitsBlock = null
  private var header: Array[String] = null
  private var nrowsLong : Long = 0L
  private var rowSizeLong : Long = 0L
  private var recordKey: LongWritable = null
  // private var recordValue: List[Row] = null
  private var recordValue: List[Row] = null

  override def close() {
    if (fB.data != null) {
      fB.data.close()
    }
  }

  override def getCurrentKey: LongWritable = {
    recordKey
  }

  override def getCurrentValue: List[Row] = {
    recordValue
  }

  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0.toFloat
      case _ => Math.min(
        ((currentPosition - splitStart) / (splitEnd - splitStart)).toFloat, 1.0
      ).toFloat
    }
  }

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {
    // the file input
    val fileSplit = inputSplit.asInstanceOf[FileSplit]

    // the byte position this fileSplit starts at
    // Need to move to an HDU and skip the header to jump to the data straight.
    splitStart = fileSplit.getStart

    // splitEnd byte marker that the fileSplit ends at

    // the actual file we will be reading from
    val file = fileSplit.getPath
    // job configuration
    val conf = context.getConfiguration
    // check compression
    val codec = new CompressionCodecFactory(conf).getCodec(file)
    if (codec != null) {
      throw new IOException("FixedLengthRecordReader does not support reading compressed files")
    }
    // get the filesystem
    // val fs = file.getFileSystem(conf)
    // open the File --> Make Fits!
    // fileInputStream = fs.open(file)
    fB = new FitsBlock(file, conf, 1)
    header = fB.readHeader

    nrowsLong = fB.getNRows(header)
    rowSizeLong = fB.getSizeRowBytes(header)

    // get the record length nlines
    // recordLength = FitsFileInputFormat.getRecordLength(context)
    // println(recordLength)
    recordLength = 32 * 1024 * 1024 //100 * rowSizeLong.toInt
    // recordLength = 20 * 1000

    splitEnd = splitStart + fileSplit.getLength//splitStart + fileSplit.getLength
    // splitEnd = if (nrowsLong < splitStart + fileSplit.getLength) {
    //   nrowsLong
    // } else splitStart + fileSplit.getLength

    // println(splitStart + nrowsLong * rowSizeLong/69L)
    // println(splitStart + fileSplit.getLength)
    // set our current position
    currentPosition = splitStart
  }

  override def nextKeyValue() : Boolean = {
    if (recordKey == null) {
      recordKey = new LongWritable()
    }
    // the key is a linear index of the record, given by the
    // position the record starts divided by the record length
    recordKey.set(currentPosition / recordLength)

    // the recordValue to place the Row into
    // if (recordValue == null) {
    //   // recordValue = new BytesWritable(new Array[Byte](recordLength))
    //   // recordValue = new ObjectWritable(new Array[Object](recordLength))
    //   // recordValue = Row.empty
    //   recordValue = new Array[Byte](recordLength)
    // }
    // read a record if the currentPosition is less than the split end
    if (currentPosition < splitEnd) {

      // Store the record
      // recordValue = Array(hdu.getRow(currentPosition.toInt)
      // .map {
      //   case x : Array[_] => x.asInstanceOf[Array[_]](0)
      //   case x : String => x
      //   }
      // // Map to Row to allow the conversion to DF later on
      // ).map { x => Row.fromSeq(x)}.toList(0)

      // recordValue = Row.fromSeq(fB.readLine(header))
      // recordValue = fB.readLines(header, recordLength / rowSizeLong)
      // println(recordValue.size)
      // val buffer = recordValue.getBytes
      // fB.data.readFully(buffer)

      // fB.data.read(recordValue, 0, recordLength)

      recordValue = (for {
        i <- 0 to recordLength / rowSizeLong.toInt

      } yield(Row.fromSeq(fB.readLine(header)))).toList
      //yield(Row.fromSeq(fB.readLine(header)))).toList
      //yield(i)).toList

      // recordValue.sliding(20)

      // update our current position
      currentPosition = currentPosition + recordLength

      // return true
      return true
    }
    println(s"Start: $splitStart EndPosition : " + currentPosition.toString)
    false
  }
}
