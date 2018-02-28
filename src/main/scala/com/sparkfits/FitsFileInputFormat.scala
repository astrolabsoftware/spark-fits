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

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row

private[sparkfits] object FitsFileInputFormat {
  /** Property name to set in Hadoop JobConfs for record length */
  val RECORD_LENGTH_PROPERTY = "com.sparkfits.FitsFileInputFormat.recordLength"

  /** Retrieves the record length property from a Hadoop configuration */
  def getRecordLength(context: JobContext): Int = {
    context.getConfiguration.get(RECORD_LENGTH_PROPERTY).toInt
  }
}

class FitsFileInputFormat extends FileInputFormat[LongWritable, List[List[_]]] {

  // private var recordLength = -1
  //
  // /**
  //  * Override of isSplitable to ensure initial computation of the record length
  //  */
  // override def isSplitable(context: JobContext, filename: Path): Boolean = {
  //   if (recordLength == -1) {
  //     recordLength = FitsFileInputFormat.getRecordLength(context)
  //   }
  //   if (recordLength <= 0) {
  //     println("record length is less than 0, file cannot be split")
  //     false
  //   } else {
  //     true
  //   }
  // }
  //
  // /**
  //   * This input format overrides computeSplitSize() to make sure that each split
  //   * only contains full records. Each InputSplit passed to FitsRecordReader
  //   * will start at the first byte of a record, and the last byte will be
  //   * the last byte of a record.
  //   */
  // override def computeSplitSize(blockSize: Long, minSize: Long, maxSize: Long): Long = {
  //   val defaultSize = super.computeSplitSize(blockSize, minSize, maxSize)
  //   // If the default size is less than the length of a record, make it equal to it
  //   // Otherwise, make sure the split size is as close to possible as the default size,
  //   // but still contains a complete set of records, with the first record
  //   // starting at the first byte in the split and the last
  //   // record ending with the last byte.
  //   if (defaultSize < recordLength) {
  //     recordLength.toLong
  //   } else {
  //     (Math.floor(defaultSize / recordLength) * recordLength).toLong
  //   }
  // }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext):
    RecordReader[LongWritable, List[List[_]]] = new FitsRecordReader()

    /**
   * Allow minPartitions set by end-user in order to keep compatibility with old Hadoop API,
   * which is set through setMaxSplitSize
   */
  def setMinPartitions(context: JobContext, minPartitions: Int) {
    // val files = listStatus(context).asScala
    // val totalLen = files.map(file => if (file.isDirectory) 0L else file.getLen).sum
    val maxSplitSize = 2
    // super.setMaxSplitSize(maxSplitSize)
  }
}
