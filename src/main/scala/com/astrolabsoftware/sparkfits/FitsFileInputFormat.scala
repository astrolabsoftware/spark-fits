/*
 * Copyright 2018 AstroLab Software
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
package com.astrolabsoftware.sparkfits

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.sql.Row

private[sparkfits] object FitsFileInputFormat {

  /**
    * `FitsFileInputFormat` extends `FileInputFormat` by creating a custom
    * RecordReader for FITS file. Note that the output class type is
    * KEY: LongWritable, VALUE: Seq[Row]. VALUE will be converted later
    * into Row to easily create DataFrame.
    */
  class FitsFileInputFormat extends FileInputFormat[LongWritable, Seq[Row]] {

    /**
      * Override the RecordReader class with our custom RecordReader for FITS file
      *
      * @param split : (InputSplit)
      *   Represents the data to be processed by an individual Mapper.
      * @param context : (TaskAttemptContext)
      *   The context for task attempts (see Hadoop code source).
      */
    override def createRecordReader(split: InputSplit, context: TaskAttemptContext):
      RecordReader[LongWritable, Seq[Row]] = new FitsRecordReader()
  }
}
