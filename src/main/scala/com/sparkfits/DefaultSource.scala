package com.sparkfits

import org.apache.spark.SparkException
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources._

class DefaultSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    val paths = parameters.get("path") match {
      case Some(x) => x.split(",").map(_.trim)
      case None => sys.error("'path' must be specified.")
    }

    val hdu = parameters.get("HDU") match {
      case Some(x) => x
      case None => throw new SparkException("You must provide a HDU")
    }

    // val extensions = parameters.getOrElse("extension", "h5").split(",").map(_.trim)
    // val chunkSize = parameters.getOrElse("chunk size", "10000").toInt

    // new FitsRelation(paths, hdu, extensions, chunkSize)(sqlContext)
    new FitsRelation(paths, hdu.toInt)(sqlContext)
  }
}

class DefaultSource15 extends DefaultSource with DataSourceRegister {

  /* Extension of spark.hdf5.DefaultSource (which is Spark 1.3 and 1.4 compatible) for Spark 1.5.
   * Since the class is loaded through META-INF/services we can decouple the two to have
   * Spark 1.5 byte-code loaded lazily.
   *
   * This trick is adapted from spark elasticsearch-hadoop data source:
   * <https://github.com/elastic/elasticsearch-hadoop>
   */
  override def shortName(): String = "fits"

}
