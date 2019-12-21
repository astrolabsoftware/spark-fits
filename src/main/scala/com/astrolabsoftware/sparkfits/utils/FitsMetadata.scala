package com.astrolabsoftware.sparkfits.utils

import com.astrolabsoftware.sparkfits.FitsLib
import com.astrolabsoftware.sparkfits.FitsLib.Fits
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import org.apache.spark.sql.execution.datasources.PartitionedFile

class FitsMetadata(partitionedFile: PartitionedFile, conf: Configuration) {

  val log = LogManager.getRootLogger
  val path = new Path(partitionedFile.filePath)
  private val fits = new Fits(path, conf, conf.getInt("hdu", -1))
  private val header = fits.blockHeader
  private var notValid = false
  val keyValues = FitsLib.parseHeader(header)
  if (keyValues("NAXIS").toInt == 0){
    conf.get("mode") match {
      case "PERMISSIVE" =>
        log.warn(s"Empty HDU for ${path}")
        notValid = true
      case "FAILFAST" =>
        log.warn(s"Empty HDU for ${path}")
        log.warn(s"Use option('mode', 'PERMISSIVE') if you want to discard all empty HDUs.")
      case _ =>
    }
  }

  val nrowsLong = fits.hdu.getNRows(keyValues)
  val rowSizeInt = fits.hdu.getSizeRowBytes(keyValues)
  val rowSizeLong = rowSizeInt.toLong





}
