/*
 * Copyright 2019 AstroLab Software
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
package com.astrolabsoftware.sparkfits.v2

import com.astrolabsoftware.sparkfits.FitsLib.Fits
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.SerializableConfiguration

class FitsScan(
                sparkSession: SparkSession,
                conf: Configuration,
                schema: StructType
              ) extends Scan with Batch {

  println("Using the V2 for read")

  override def toBatch: Batch = this

  // FITS does not support column pruning or other optimizations at the file level.
  // So schema won't change at run-time
  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    partitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Broadcast the confs which will have the file boundaries and headers
    // for the re-use on the executor side
    val broadCastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(conf))
    new FitsPartitionReaderFactory(sparkSession, broadCastedConf, schema)
  }

  protected def partitions: Seq[FilePartition] = {
    if (conf.getBoolean("implemented", true)) {
      val partitionedFiles = getPartitionedFiles()
      // Sort by length so that bigger blocks are scheduled first
      val sortedPartitionedFiles = partitionedFiles.sortBy(_.length)
      val splitBytes = maxSplitBytes(sparkSession, partitionedFiles)
      // Handle the case when there is just one file and its size is less than then splitBytes
      FilePartition.getFilePartitions(sparkSession, sortedPartitionedFiles, splitBytes)
    } else {
      Seq.empty
    }
  }

  private def getPartitionedFiles(): Seq[PartitionedFile] = {
    // FixMe: For a really large number of files, the Driver will get stuck while listing
    //  the files itself for block storage like S3. If not that, driver definitely
    //  get stuck while iterating over each file, and computing the boundaries
    //  We can launch a spark job to list the files and compute the boundaries distributively
    //  :Just_A_Thought:
    val files = conf.get("listOfFitsFiles").split(",")
    files.map {
      file =>
        val path = new Path(file)
        val fits = new Fits(path, conf, conf.getInt("hdu", 0))
        val boundaries = fits.getBlockBoundaries
        // Register the header and block boundaries for re-use later
        fits.registerHeader
        fits.blockBoundaries.register(path, conf)
        // ToDO: Check this once - InternalRow.empty
        PartitionedFile(InternalRow.empty, file, boundaries.dataStart, boundaries.blockStop - boundaries.dataStart)
    }
  }

  /**
    * Borrowed from [[org.apache.spark.sql.execution.datasources.FilePartition$#maxSplitBytes]]
    */
  def maxSplitBytes(sparkSession: SparkSession,
                     partitionedFiles: Seq[PartitionedFile]): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val totalBytes = partitionedFiles.map(_.length + openCostInBytes).sum
    val bytesPerCore = totalBytes / defaultParallelism
    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }
}
