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

import java.nio.charset.StandardCharsets

import scala.util.Try

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import com.astrolabsoftware.sparkfits.FitsHdu._

/**
  * This is the beginning of a FITS library in Scala.
  * You will find a large number of methodes to manipulate Binary Table HDUs.
  * There is no support for image HDU for the moment.
  */
object FitsLib {

  // Define some FITS standards.

  // Standard size of a block (bytes)
  val FITSBLOCK_SIZE_BYTES = 2880

  // Size of one row in header (bytes)
  val FITS_HEADER_CARD_SIZE = 80

  // Size of KEYWORD (KEYS) in FITS (bytes)
  val MAX_KEYWORD_LENGTH = 8

  // 8 bits in one byte
  val BYTE_SIZE = 8

  // Separator used for values from blockBoundaries
  val separator = ";;"

  /**
    * Class to hold block boundaries. These values are computed at first
    * file scan then encoded to be broadcasted to all datanodes through
    * the Hadoop configuration block.
    *
    * @param headerStart : (Long)
    *   Starting byte of the header blocks
    * @param dataStart : (Long)
    *   Starting byte of the data blocks
    * @param dataStop : (Long)
    *   Last byte of non-zero data (could be in a middle of a data block)
    * @param blockStop : (Long)
    *   Last byte of the data blocks (data blocks are multiple of 2880 bytes)
    */
  case class FitsBlockBoundaries(headerStart: Long = 0L, dataStart: Long = 0L,
    dataStop: Long = 0L, blockStop: Long = 0L) {

    /**
      * Register the boundaries of the HDU in the Hadoop configuration.
      * By doing this, we broadcast the values to the executors.
      * It is sent as a long String, and can be read properly
      * afterwards using retrieveBlockBoundaries. Make sure you use the same
      * separators.
      *
      */
    def register(hdfsPath: Path, conf: Configuration) = {
      val str = this.productIterator.toArray.mkString(separator)

      conf.set(hdfsPath + "_blockboundaries", str)
    }

    /**
      * Check whether the data block is empty based on start/end indices.
      *
      * @return (Boolean) True if the data block has zero size. False otherwise.
      *
      */
    def empty: Boolean = {
      dataStart == dataStop
    }
  }

  /**
    * Decompose each line of the header into (key, value).
    * Note the white space before the slash to include colnames with slash:
    *   a=toto/tutu    / a comment
    * TTYPEn would be `toto/tutu`
    *
    * I reckon This is not very robust though as rows such as
    *   a=toto /tutu    / a comment
    * where the value of TTYPEn is expected to be `toto / tutu` would not
    * be considered, and it would return only `toto`.
    *
    *
    * @param header : (Array[String])
    *   The header of the HDU.
    * @return (Map[String, String]), map array with (keys, values).
    *
    */
  def parseHeader(header : Array[String]) : Map[String, String] = {
    header.map(x => x.split("="))
      .filter(x => x.size > 1)
      .map(x => (x(0).trim(), x(1).split(" /")(0).trim()))
      .toMap
  }

  /**
    * This is a better code than above, but it does not work if
    * there is no comment section on the card...
    *   a=toto/tutu    / a comment --> OK
    *   a=toto/tutu                --> NOT OK
    * Unfortunately, both exist :-(
    *
    * @param header : (Array[String])
    *   The header of the HDU.
    * @return (Map[String, String]), map array with (keys, values).
    *
    */
  // def parseHeader(header : Array[String]) : Map[String, String] = {
  //   header.map(x => x.split("="))
  //     .filter(x => x.size > 1)
  //     .map{x =>
  //       val head = x(0).trim()
  //       val ttype_ = x(1).split("/").map(y => y.trim())
  //
  //       val ttype = ttype_.slice(0, ttype_.size - 1).mkString("/")
  //
  //       (head, ttype)
  //     }.toMap
  // }

  /**
    *
    * Remove single quotes around a string, and trim the resulting string.
    * e.g. "'toto '" would return "toto".
    *
    * @param s : (String)
    *   Input string.
    * @return (String), Trimmed input String without the starting and
    *   ending single quotes.
    */
  def shortStringValue(s: String): String = {
    if (s.startsWith("'") && s.endsWith("'")) {
      s.slice(1, s.size - 1).trim
    } else s
  }

  /**
    * Main class to handle a HDU of a fits file. Main features are
    *   - Retrieving a HDU (block) of data
    *   - Split the HDU into a header and a data block
    *   - Get informations on data from the header (column name, element types, ...)
    *
    *
    * @param hdfsPath : (Path)
    *   Hadoop path containing informations on the file to read.
    * @param conf : (Configuration)
    *   Hadoop configuration containing informations on the run.
    * @param hduIndex : (Int)
    *   Index of the HDU to read (zero-based).
    *
    */
  class Fits(hdfsPath : Path, conf : Configuration, hduIndex : Int) {

    // Open the data
    val fs = hdfsPath.getFileSystem(conf)
    val data = fs.open(hdfsPath)

    // Check that the HDU asked is below the max HDU index.
    // Check only header no registered yet
    val numberOfHdus = if (conf.get(hdfsPath + "_header") == null) {
      getNHDU
    } else hduIndex + 1

    val isHDUBelowMax = hduIndex < numberOfHdus
    isHDUBelowMax match {
      case true => isHDUBelowMax
      case false => throw new AssertionError(s"""
        HDU number $hduIndex does not exist!
        """)
    }

    // Compute the bound and initialise the cursor
    // indices (headerStart, dataStart, dataStop) in bytes.
    val key = conf.get(hdfsPath + "_blockboundaries")
    val blockBoundaries = if (key != null) {
      // Retrieve the boundaries as a String, split it, and cast to Long
      val arr = key.split(separator).map(x => x.toLong)
      FitsBlockBoundaries(arr(0), arr(1), arr(2), arr(3))
    } else {
      getBlockBoundaries
    }

    // Initialise the HDU: is it empty?
    val empty_hdu = blockBoundaries.empty

    // Get the header and set the cursor to its start.
    val blockHeader = if (conf.get(hdfsPath + "_header") != null) {
      retrieveHeader
    } else readFullHeaderBlocks
    val fullHeaderBlock = blockHeader
    resetCursorAtHeader

    // Check whether we know the HDU type.
    val hduType = getHduType
    val hdu: HDU = hduType match {
      case "BINTABLE" => handleBintable
      case "IMAGE" => handleImage
      case "TABLE" => AnyHDU()
      case "EMPTY" => AnyHDU()
      case _ => throw new AssertionError(s"""
        $hduType HDU not yet implemented!
        """)
    }

    if (conf.get("recordlength") != null) {
      val keyValues = parseHeader(blockHeader)
      val recordLength = conf.get("recordlength").toInt

      // Set the rowsize to recordLength in case of trouble,
      // e.g empty HDU (checks are performed later in FitsRecordReader)
      val rowSize = Try{hdu.getSizeRowBytes(keyValues)}.getOrElse{recordLength}

      if (recordLength < rowSize) {
        throw new AssertionError(s"""
          You specified a recordLength option too small compared to the FITS row size:
          $recordLength B < $rowSize B """)
      }
    }

    /**
      * Give access to full header of HDU
      *
      * @return (Array[String])
      *
      */
    def getHDUFullHeader : Array[String] = {
      fullHeaderBlock
    }

    /**
      * Give access to methods concerning Image HDU.
      *
      * @return (ImageHDU)
      *
      */
    def handleImage : FitsHduImage.ImageHDU = {
      // Return an Image HDU
      FitsHduImage.ImageHDU(blockHeader)
    }

    /**
      * Give access to methods concerning BinTable HDU.
      *
      * @return (BintableHDU)
      *
      */
    def handleBintable : FitsHduBintable.BintableHDU = {
      // Grab only columns specified by the user
      val selectedColNames = if (conf.get("columns") != null) {
        conf.getStrings("columns").deep.toList.asInstanceOf[List[String]]
      } else null

      FitsHduBintable.BintableHDU(blockHeader, selectedColNames)
    }

    /**
      * Compute the indices of the first and last bytes of the HDU:
      * hdu_start=header_start, data_start, data_stop, hdu_stop
      *
      * @return (FitsBlockBoundaries), Instance of FitsBlockBoundaries
      *   initialised with the boundaries of the FITS HDU (header+data).
      *
      */
    def getBlockBoundaries: FitsBlockBoundaries = {

      // Initialise the cursor position at the beginning of the file
      data.seek(0)
      var hduTmp = 0

      // Initialise the boundaries
      var headerStart : Long = 0
      var dataStart : Long = 0
      var dataStop : Long = 0
      var blockStop : Long = 0

      // Loop over HDUs, and stop at the desired one.
      do {
        // Initialise the offset to the header position
        headerStart = data.getPos

        val localHeader = readFullHeaderBlocks

        // Data block starts after the header
        dataStart = data.getPos

        val keyValues = parseHeader(localHeader)

        // Size of the data block in Bytes.
        // Skip Data if None (typically HDU=0)
        val data_len = Try {
          getDataLen(keyValues)
        }.getOrElse(0L)

        // Where the actual data stopped
        dataStop = dataStart + data_len

        // Store the final offset
        // FITS is made of blocks of size 2880 bytes, so we might need to
        // pad to jump from the end of the data to the next header.
        blockStop = if ((dataStart + data_len) % FITSBLOCK_SIZE_BYTES == 0) {
          dataStop
        } else {
          dataStop + FITSBLOCK_SIZE_BYTES -  (dataStop) % FITSBLOCK_SIZE_BYTES
        }

        // Move to the another HDU if needed
        hduTmp = hduTmp + 1

        // S3 filesystem complains in the case of
        // seek(len(file)), while HDFS does not.
        Try{
          data.seek(blockStop)
        }.getOrElse{data.seek(blockStop - 1)}

      } while (hduTmp < hduIndex + 1)

      // Reposition the cursor at the beginning of the block
      data.seek(headerStart)

      // Return boundaries (included):
      // hdu_start=headerStart, dataStart, dataStop, hdu_stop
      val fb = FitsBlockBoundaries(headerStart, dataStart, dataStop, blockStop)

      // Return the instance
      fb
    }

    /**
      * Check the type of HDU. Available: BINTABLE, IMAGE, or EMPTY.
      * If not registered, returns NOT UNDERSTOOD.
      * Note: Not working if an image is stored in a primary HDU... TBD.
      *
      * @return (String) The kind of HDU data.
      */
    def getHduType : String = {

      // Get the header NAMES
      val colNames = parseHeader(blockHeader)

      // Check if the HDU is empty, a table or an image
      var isBintable = colNames.filter(
        x=>x._2.contains("BINTABLE")).values.toList.size > 0
      var isTable = colNames.filter(
        x=>x._2.contains("TABLE")).values.toList.size > 0
      var isImage = colNames.filter(
        x=>x._2.contains("IMAGE")).values.toList.size > 0
      val isEmpty = empty_hdu

      // Case where you are neither a bintable, nor a table, nor an image...
      // This happens if you are an zombie, or most likely in the HDU 0.
      // For some obscure reason, the data type is not declared in the HDU 0,
      // and therefore the previous piece of code cannot detect the data type.
      // In 99% of the case, people store image in this zeroth HDU, so I will
      // assume. This is not bug-free of course, and I might have to change this
      // in the future. The reason why I'm spending plenty of time to describe
      // a potential error instead of fixing it is that I do not know exactly
      // how to fix it. Any ideas?
      if (!isBintable && !isTable && !isImage && !isEmpty) {
        // println("""
        //   You are in the zeroth HDU with non-empty data block.
        //   We are assuming that the data is an image HDU.
        //   If this is not the case, goto FitsLib.scala.
        //   """)
        isImage = true
      }

      val fitstype = if (isBintable) {
        "BINTABLE"
      } else if (isTable) {
        "TABLE"
      } else if (isImage) {
        "IMAGE"
      } else if (isEmpty) {
        "EMPTY"
      } else {
        "NOT UNDERSTOOD"
      }
      fitstype
    }

    /**
      * Compute the size of a data block.
      *
      * @param keyValues : (Map[String, String])
      *   Values from the header (see parseHeader)
      * @return (Long) Size of the corresponding data block
      *
      */
    def getDataLen(keyValues: Map[String, String]): Long = {
      // Initialise the data size
      var dataSize = 0L

      // Number of dimensions
      val naxis = keyValues("NAXIS").toInt

      // Accumulate the size dimension-by-dimension
      if (naxis > 0) {
        dataSize = math.abs(keyValues("BITPIX").toLong) / 8L
        for (a <- 1 to naxis) {
          val axis = keyValues(s"NAXIS${a}").toLong
          dataSize = dataSize * axis
        }
      }

      // Return the data size
      dataSize
    }

    /**
      * Return the number of HDUs in the FITS file.
      *
      * @return (Int) the number of HDU.
      *
      */
    def getNHDU : Int = {

      // Initialise the file
      data.seek(0)
      var currentHduIndex = 0

      var hasData : Boolean = false
      var datalen = 0L

      // Loop over all HDU, and exit.
      do {
        val localHeader = readFullHeaderBlocks

        // If the header cannot be read,
        if (localHeader.size > 0) {
          hasData = true

          // Size of the data block in Bytes.
          // Skip Data if None (typically HDU=0)
          datalen = Try {
            getDataLen(parseHeader(localHeader))
          }.getOrElse(0L)

          // Store the offset to the next HDU
          // FITS is made of blocks of size 2880 bytes, both for the headers and for the data
          // if datalen is not null, it must be justified to an integer number of blocks
          val skipBytes = if (datalen % FITSBLOCK_SIZE_BYTES == 0) {
            datalen
          }
          else {
            datalen + FITSBLOCK_SIZE_BYTES - (datalen % FITSBLOCK_SIZE_BYTES)
          }

          // S3 filesystem complains in the case of
          // seek(len(file)), while HDFS does not.
          Try{
            data.seek(data.getPos + skipBytes)
          }.getOrElse{data.seek(data.getPos + skipBytes - 1)}

          // Move to the another HDU if needed
          currentHduIndex = currentHduIndex + 1
        }
        else {
          hasData = false
        }

      } while (hasData)

      // Return the number of HDU.
      currentHduIndex
    }

    /**
      * Place the cursor at the beginning of the header of the block
      *
      */
    def resetCursorAtHeader : Unit = {
      // Place the cursor at the beginning of the block
      data.seek(blockBoundaries.headerStart)
    }

    /**
      * Place the cursor at the beginning of the data of the block
      *
      */
    def resetCursorAtData : Unit = {
      // Place the cursor at the beginning of the block
      data.seek(blockBoundaries.dataStart)
    }

    /**
      * Set the cursor at the `position` (byte index, Long).
      *
      * @param position : (Long)
      *   The byte index to seek in the file.
      *
      */
    def setCursor(position : Long) = {
      data.seek(position)
    }

    /**
      * Read all header blocks of a HDU. The cursor needs to be at the start of
      * the header.
      *
      * @return (Array[String) the header is an array of Strings, each String
      *   being one line of the header.
      */
    def readFullHeaderBlocks : Array[String] = {
      // Initialise the header
      var header = Array.newBuilder[String]
      var ending = false

      // Counter for the number of blocks (multiple of 2880 bytes)
      var blockNumber = 0

      // Loop until we reach the end of the header
      do {

        // Read one block
        val block = readHeaderBlock
        ending = block.size == 0

        if (!ending)
          {
            var first = true
            var last = ""

            for
              {
                line <- block
                if line.trim() != ""} {

                if (line.trim() == "END") ending = true

                if (first)
                {
                  first = false
                }

                last = line
                header += line
              }
          } else {
            ending = true
          }

        blockNumber += 1
      } while (!ending)
      header.result
    }

    /**
      * Read one header block of a HDU. The cursor needs to be at the start of
      * the block. We assume that each header row has a standard
      * size of 80 Bytes, and the total size of the header is 2880 Bytes.
      *
      * @return (Array[String) the header block is an array of Strings,
      *   each String being one line of the header.
      */
    def readHeaderBlock : Array[String] = {
      // Initialise a line of the header
      var buffer = new Array[Byte](FITSBLOCK_SIZE_BYTES)

      // Initialise the block
      val header = Array.newBuilder[String]

      // Catch end of the file if necessary
      val len = try {
        data.read(buffer, 0, FITSBLOCK_SIZE_BYTES)
      } catch {
        case e : Exception => { e.printStackTrace; 0}
      }

      // Decode the header block
      if (len > 0) {
        val maxLines = FITSBLOCK_SIZE_BYTES / FITS_HEADER_CARD_SIZE

        var inBlock = true

        var pos = 0

        for {
          i <- 0 to maxLines-1
          if inBlock} {
          val line = new String(
            buffer.slice(pos, pos + FITS_HEADER_CARD_SIZE),
            StandardCharsets.UTF_8)

          header += line

          // Header ends with the String END
          if (line.trim() == "END") {
            inBlock = false
          }
          pos += FITS_HEADER_CARD_SIZE
        }
      }

      header.result
    }

    /**
      * Register the header in the Hadoop configuration.
      * By doing this, we broadcast the header to the executors.
      * The header is sent as a long String, and can be read properly
      * afterwards using retrieveHeader. Make sure you use the same
      * separators.
      *
      */
    def registerHeader : Unit = {
      conf.set(hdfsPath + "_header", blockHeader.mkString(separator))
    }

    /**
      * Retrieve the header from the Hadoop configuration.
      * Make sure you use the same separators as in registerHeader.
      *
      * @return the header as Array[String]. See readHeader.
      *
      */
    def retrieveHeader : Array[String] = {

      conf.get(hdfsPath + "_header").split(separator)
    }

    /**
      * Convert binary row into row. You need to have the cursor at the
      * beginning of a row. Example
      * {{{
      * // Set the cursor at the beginning of the data block
      * setCursor(BlockBoundaries.dataStart)
      * // Initialise your binary row
      * val buffer = Array[Byte](size_of_one_row_in_bytes)
      * // Read the first binary row into buffer
      * data.read(buffer, 0, size_of_one_row_in_bytes)
      * // Convert buffer
      * val myrow = getRow(buffer)
      * }}}
      *
      * @param buf : (Array[Byte])
      *   Row of bytes read from the data block.
      * @return (List[_]) The row as list of elements (float, int, string, etc.)
      *   with types as given by the header.
      *
      */
    def getRow(buf : Array[Byte]): List[_] = {
      if (hdu.implemented) {
        hdu.getRow(buf)
      } else null
    }

    /**
      * Get the comments of the header.
      * We assume the comments are written after a backslash (\).
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @return (Map[String, String]), a map of keyword/comment.
      *
      */
    def getHeaderComments(header : Array[String]) : Map[String, String] = {
      val headerMap = header.map(x => x.split("/"))
          .filter(x => x.size > 1)
          // (KEYWORD, COMMENTS)
          .map(x => (x(0).split("=")(0).trim(), x(1).trim()))
          .toMap

      // Return the Map.
      headerMap
    }
  }
}
