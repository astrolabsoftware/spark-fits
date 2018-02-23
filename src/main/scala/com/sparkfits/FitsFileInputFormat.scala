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
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.sql.Row
import com.sparkfits.FitsRecordReader

private[sparkfits] object FitsFileInputFormat {
  /** Property name to set in Hadoop JobConfs for record length */
  val RECORD_LENGTH_PROPERTY = "org.apache.spark.input.FixedLengthBinaryInputFormat.recordLength"

  /** Retrieves the record length property from a Hadoop configuration */
  def getRecordLength(context: JobContext): Int = {
    context.getConfiguration.get(RECORD_LENGTH_PROPERTY).toInt
  }
}

class FitsFileInputFormat extends FileInputFormat[LongWritable, List[List[_]]] {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext):
    RecordReader[LongWritable, List[List[_]]] = new FitsRecordReader()
}
