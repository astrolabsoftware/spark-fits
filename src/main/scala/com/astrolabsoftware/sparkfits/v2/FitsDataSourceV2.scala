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