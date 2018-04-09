package com.sparkfits

import org.apache.spark.SparkException
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources._
import org.apache.hadoop.conf.Configuration

class DefaultSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new FitsRelation(parameters)(sqlContext)
  }
}
