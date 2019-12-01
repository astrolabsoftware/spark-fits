package com.astrolabsoftware.sparkfits.v2

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FitsDataSourceV2 extends FileDataSourceV2 {

  override def shortName() = "fits"

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(paths)
    FitsTable(tableName, sparkSession, options, paths, Some(schema), fallbackFileFormat)
  }

  // Still have to figure this out
  override def fallbackFileFormat: Class[_ <: FileFormat] = ???

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(paths)
    FitsTable(tableName, sparkSession, options, paths, None, fallbackFileFormat)
  }
}