package com.astrolabsoftware.sparkfits.utils

import com.astrolabsoftware.sparkfits.FitsLib
import com.astrolabsoftware.sparkfits.FitsLib.Fits
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import org.apache.spark.sql.execution.datasources.PartitionedFile

import scala.util.Try

class FitsMetadata(partitionedFile: PartitionedFile, val index: Int, conf: Configuration) {

  val log = LogManager.getRootLogger
  val path = new Path(partitionedFile.filePath)
  private[sparkfits] val fits = new Fits(path, conf, conf.getInt("hdu", -1))
  private[sparkfits] val startStop = fits.blockBoundaries
  private val header = fits.blockHeader
  private[sparkfits] var notValid = false
  val keyValues = FitsLib.parseHeader(header)
  if (keyValues("NAXIS").toInt == 0) {
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

  // Get the record length in Bytes (get integer!). First look if the user
  // specify a size for the recordLength. If not, set it to max(1 Ko, rowSize).
  // If the HDU is an image, the recordLength is the row size (NAXIS1 * nbytes)
  val recordLengthFromUser = Try{conf.get("recordlength").toInt}
    .getOrElse{
      if (fits.hduType == "IMAGE") {
        rowSizeInt
      } else {
        // set it to max(1 Ko, rowSize)
        math.max((1 * 1024 / rowSizeInt) * rowSizeInt, rowSizeInt)
      }
    }

  val nrowsLong = fits.hdu.getNRows(keyValues)
  val rowSizeInt = fits.hdu.getSizeRowBytes(keyValues)
  val rowSizeLong = rowSizeInt.toLong

  // For Table, seek for a round number of lines for the record
  // ToDo: Cases when the user has given the record length
  private var recordLength = (recordLengthFromUser / rowSizeInt) * rowSizeInt

  // Make sure that the recordLength is not bigger than the block size!
  // This is a guard for small files.
  recordLength = if ((recordLength / rowSizeInt)  < nrowsLong.toInt) {
    // OK less than the total number of lines
    recordLength
  } else {
    // Small files, one record is the entire file.
    nrowsLong.toInt * rowSizeLong.toInt
  }
  // Move to the starting binary index
  fits.data.seek(startStop.dataStart)
}
