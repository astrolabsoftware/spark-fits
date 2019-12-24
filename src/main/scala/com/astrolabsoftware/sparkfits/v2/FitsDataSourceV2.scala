package com.astrolabsoftware.sparkfits.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FitsDataSourceV2 extends TableProvider with DataSourceRegister {

  // ToDo: Use the name "fits" and still resolve v1 vs v2 somehow
  override def shortName() = "fitsv2"

  lazy val sparkSession = SparkSession.active

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    FitsTable(sparkSession, options, Some(schema))
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    FitsTable(sparkSession, options, None)
  }
}