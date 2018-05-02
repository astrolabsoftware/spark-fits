/*
 * Copyright 2018 Julien Peloton
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
package com.sparkfits

import java.io.EOFException
import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._

import scala.util.{Try, Success, Failure}

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

  // Separator used for values from blockBoundaries
  val separator = ";;"

  /**
    * Return the (key,values) of the header.
    *
    * @param header : (Array[String])
    *   The header of the HDU.
    * @return (Map[String, String]), map array with (keys, values).
    *
    */
  def parseHeader(header : Array[String]) : Map[String, String] = {
    header.map(x => x.split("="))
      .filter(x => x.size > 1)
      .map(x => (x(0).trim(), x(1).split("/")(0).trim()))
      .toMap
  }

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

      conf.set(hdfsPath + "blockboundaries", str)
    }

    /**
      * Set starting byte to last byte (empty block).
      *
      */
    def empty = {
      dataStart == dataStop
    }

    /**
      * Override the method toString to print block boundaries info.
      * Useful for debugging.
      */
    override def toString: String = {
      s"[headerStart=$headerStart dataStart=$dataStart dataStop=$dataStop blockStop=$blockStop]"
    }
  }

  /**
    * Trait containing generic informations concerning HDU informations.
    * This includes for example number of rows, size of a row,
    * number of columns, types of elements, and methods to access elements.
    *
    */
  trait HDU {

    /**
      * Whether the HDU is implemented in the library.
      * @return (Boolean)
      */
    def implemented: Boolean

    /** Geometrical informations about the HDU (size, element types, etc) */
    def getNRows(keyValues: Map[String, String]) : Long
    def getSizeRowBytes(keyValues: Map[String, String]) : Int
    def getNCols(keyValues : Map[String, String]) : Long
    def getColTypes(keyValues : Map[String, String]): List[String]

    // Useful to convert the Header into a DF schema.
    def listOfStruct : List[StructField]

    // Methods to access the elements of the data block.
    def getRow(buf: Array[Byte]): List[Any]
    def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any
  }

  /**
    *
    * Generic class extending Infos concerning dummy HDU (e.g. not implemented).
    * Set all variables and methods to null/0/false.
    */
  case class AnyHDU(hduType: String) extends HDU {

    // Not implemented
    def implemented: Boolean = {false}

    // Zero-size, null type elements
    def getNRows(keyValues: Map[String, String]) : Long = {0L}
    def getSizeRowBytes(keyValues: Map[String, String]) : Int = {0}
    def getNCols(keyValues : Map[String, String]) : Long = {0L}
    def getColTypes(keyValues : Map[String, String]): List[String] = {null}

    // Null schema
    def listOfStruct : List[StructField] = {null}

    // No elements to return
    def getRow(buf: Array[Byte]): List[Any] = {null}
    def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {null}
  }

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
  class FitsBlock(hdfsPath : Path, conf : Configuration, hduIndex : Int) {

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

    // Initialise the HDU
    val empty_hdu = blockBoundaries.empty

    // Get the header and set the cursor to its start.
    val blockHeader = if (conf.get(hdfsPath + "header") != null) {
      retrieveHeader()
    } else readFullHeaderBlocks
    resetCursorAtHeader

    // Check whether we know the HDU type.
    val hduType = getHduType
    val hdu: HDU = hduType match {
      case "BINTABLE" => handleBintable(blockHeader, hduType)
      case "TABLE" => handleTable(blockHeader, hduType)
      case "IMAGE" => handleImage(blockHeader, hduType)
      case "EMPTY" => AnyHDU(hduType)
      case _ => throw new AssertionError(s"""
        $hduType HDU not yet implemented!
        """)
    }

    /**
      *
      * Return the informations concerning a Table HDU.
      *
      * @param blockHeader : (Array[String])
      *   Header of the HDU.
      * @return (TableHDU) informations concerning the Table.
      *
      */
    def handleTable(blockHeader: Array[String], hduType: String) = {
      println(s"handleTable> blockHeader=${blockHeader.toString}")
      FitsTableLib.TableHDU()
    }

    def handleImage(blockHeader: Array[String], hduType: String) = {

      val kv = parseHeader(blockHeader)

      val pixelSize = (kv("BITPIX").toInt)/8
      val dimensions = kv("NAXIS").toInt
      val axisBuilder = Array.newBuilder[Long]
      for (d <- 1 to dimensions){
        axisBuilder += kv("NAXIS" + d.toString).toLong
      }
      val axis = axisBuilder.result
      val axisStr = axis.mkString(",")

      FitsImageLib.ImageHDU(pixelSize, axis)
    }

    // Get informations on element types and number of columns.
    def handleBintable(blockHeader: Array[String], hduType: String) = {

      val selectedColNames = if (conf.get("columns") != null) {
        conf.getStrings("columns").deep.toList.asInstanceOf[List[String]]
      } else {
        null
      }

      val localHDU = FitsBintableLib.BintableHDU()
      localHDU.initialize(empty_hdu, blockHeader, selectedColNames)
    }

    def getBlockBoundaries: FitsBlockBoundaries = {

      // Initialise the cursor position at the beginning of the file
      data.seek(0)
      var hduTmp = 0

      // Initialise the boundaries
      var headerStart : Long = 0
      var dataStart : Long = 0
      var dataStop : Long = 0
      var blockStop : Long = 0

      // println(s"====== getBlockBoundaries> data.getPos=${data.getPos}")

      // Loop over HDUs, and stop at the desired one.
      do {
        // Initialise the offset to the header position
        headerStart = data.getPos

        // println(s"getBlockBoundaries-1) data.getPos=${data.getPos}")

        val localHeader = readFullHeaderBlocks

        // Data block starts after the header
        dataStart = data.getPos

        val keyValues = parseHeader(localHeader)

        // println(s"keyValues=${keyValues.mkString("\n")}")

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
        data.seek(blockStop)

      } while (hduTmp < hduIndex + 1)

      // Reposition the cursor at the beginning of the block
      data.seek(headerStart)

      // Return boundaries (included):
      // hdu_start=headerStart, dataStart, dataStop, hdu_stop
      val fb = FitsBlockBoundaries(headerStart, dataStart, dataStop, blockStop)
      // println(s"getBlockBoundaries-END> $fb")
      fb
    }

    /**
      * Check the type of HDU. Available: BINTABLE, IMAGE, or EMPTY.
      * If not registered, returns NOT UNDERSTOOD.
      * Note: Not working if an image is stored in a primary HDU... TBD.
      *
      * @return (String) The type of the HDU data.
      */
    def getHduType : String = {

      // Get the header NAMES
      val colNames = parseHeader(blockHeader)

      // Check if the HDU is empty, a table or an image
      val isBintable = colNames.filter(
        x=>x._2.contains("BINTABLE")).values.toList.size > 0
      val isTable = colNames.filter(
        x=>x._2.contains("TABLE")).values.toList.size > 0
      val isImage = colNames.filter(
        x=>x._2.contains("IMAGE")).values.toList.size > 0
      val isEmpty = empty_hdu

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
      * @param values : (Map[String, String])
      *   Values from the header (see parseHeader)
      * @return (Long) Size of the corresponding data block
      *
      */
    def getDataLen(values: Map[String, String]): Long = {
      // Initialise the data size
      var dataSize = 0L

      // Number of dimensions
      val naxis = values("NAXIS").toInt

      // Accumulate the size dimension-by-dimension
      if (naxis > 0) {
        dataSize = values("BITPIX").toLong / 8L
        for (a <- 1 to naxis) {
          val axis = values(s"NAXIS${a}").toLong
          dataSize = dataSize * axis
        }
      }

      // Return the data size
      dataSize
    }

    /**
      * Return the number of HDUs in the file.
      *
      * @return (Int) the number of HDU.
      *
      */
    def getNHDU : Int = {

      // Initialise the file
      data.seek(0)
      var currentHduIndex = 0

      var hasData : Boolean = true
      var datalen = 0L

      // Loop over all HDU, and exit.
      do {
        // println(s"\ngetNHDU-1) hdu=$currentHduIndex data.getPos=${data.getPos}")

        hasData = false
        val localHeader = readFullHeaderBlocks

        // If the header cannot be read,
        if (localHeader.size > 0) {
          hasData = true

          // Size of the data block in Bytes.
          // Skip Data if None (typically HDU=0)
          datalen = Try {
            getDataLen(parseHeader(localHeader))
          }.getOrElse(0L)

          // println(s"getNHDU-2) header=${localHeader.size} datalen=$datalen data.getPos=${data.getPos}")

          // Store the offset to the next HDU
          // FITS is made of blocks of size 2880 bytes, both for the headers and for the data
          // if datalen is not null, it must be justified to an integer number of blocks
          val skipBytes = if (datalen % FITSBLOCK_SIZE_BYTES == 0) {
            datalen
          }
          else {
            datalen + FITSBLOCK_SIZE_BYTES - (datalen % FITSBLOCK_SIZE_BYTES)
          }

          // println(s"getNHDU-3) header=${localHeader.size} data.getPos=${data.getPos} datalen=$datalen skipBytes=$skipBytes")

          data.seek(data.getPos + skipBytes)

          // Move to the another HDU if needed
          currentHduIndex = currentHduIndex + 1
        }
        else {
          // println(s"getNHDU-4) has no data")
        }

      } while (hasData)

      // println(s"getNHDU-END) data.getPos=${data.getPos} currentHduIndex=$currentHduIndex")

      // Return the number of HDU.
      currentHduIndex
    }

    /**
      * Place the cursor at the beginning of the header of the block
      *
      */
    def resetCursorAtHeader = {
      // Place the cursor at the beginning of the block
      data.seek(blockBoundaries.headerStart)
    }

    /**
      * Place the cursor at the beginning of the data of the block
      *
      */
    def resetCursorAtData = {
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
      * Read a header at a given position
      *
      * @param position : (Long)
      *   The byte index to seek in the file. Need to correspond to a valid
      *   header position. Use in combination with BlockBoundaries.headerStart
      *   for example.
      * @return (Array[String) the header is an array of Strings, each String
      *   being one line of the header.
      */
    def readHeader(position : Long) : Array[String] = {
      setCursor(position)
      readHeader
    }

    def readFullHeaderBlocks : Array[String] = {
      // println(s"readFullHeaderBlocks> data.getPos=${data.getPos}")
      var header = Array.newBuilder[String]
      var ending = false
      var blockNumber = 0
      do {
        val block = readHeaderBlock
        ending = block.size == 0
        // println(s"readFullHeaderBlocks> block.size=${block.size}")

        if (!ending)
          {
            var first = true
            var last = ""
            var s = s"readFullHeaderBlocks> add lines \n"

            for
              {
                line <- block
                if line.trim() != ""} {

                if (line.trim() == "END") ending = true

                if (first)
                {
                  s += s"$line"
                  first = false
                }

                last = line
                header += line
              }
            // println(s"$s ... $last")
          } else {
            // println("readFullHeaderBlocks> ending")
          }

        // println(s"readFullHeaderBlocks-$blockNumber) fullHeader=${header.result.size} END=${ending}")
        blockNumber += 1
      } while (!ending)
      header.result
    }

    def readHeaderBlock : Array[String] = {
      // Initialise a line of the header
      var buffer = new Array[Byte](FITSBLOCK_SIZE_BYTES)

      // println(s"readHeaderBlock> data.getPos=${data.getPos}")

      val header = Array.newBuilder[String]

      val len = try {
        data.read(buffer, 0, FITSBLOCK_SIZE_BYTES)
      } catch {
        case e : Exception => { e.printStackTrace; 0}
      }

      if (len > 0) {
        /*
        val window = 80
        val begining = new String(buffer.slice(0, window), StandardCharsets.UTF_8)
        val trailer = new String(buffer.slice(len-window, len), StandardCharsets.UTF_8)
        println(s"readHeaderBlock> data.getPos=${data.getPos} len=$len buffer=\n[${begining}\n...\n${trailer}]")
        */

        val maxLines = FITSBLOCK_SIZE_BYTES / FITS_HEADER_CARD_SIZE

        var inBlock = true

        var pos = 0

        for {
          i <- 0 to maxLines-1
          if inBlock} {
          val line = new String(buffer.slice(pos, pos + FITS_HEADER_CARD_SIZE), StandardCharsets.UTF_8)

          // println(s"=== $i $line")
          /*
          if (line.trim != "" && !line.startsWith("COMMENT")) {
            header += line
          }
          */
          header += line

          if (line.trim() == "END") {
            // println(s"readHeaderBlock-END> pos=$pos getPos=${data.getPos} modulo=${data.getPos % FITSBLOCK_SIZE_BYTES} block=${data.getPos/FITSBLOCK_SIZE_BYTES}")
            inBlock = false
          }
          pos += FITS_HEADER_CARD_SIZE
        }
      }

      header.result
    }

    /**
      * Read the header of a HDU. The cursor needs to be at the start of
      * the header. We assume that each header row has a standard
      * size of 80 Bytes, and the total size of the header is 2880 Bytes.
      *
      * @return (Array[String) the header is an array of Strings, each String
      *   being one line of the header.
      */
    def readHeader : Array[String] = {

      // Initialise a line of the header
      var buffer = new Array[Byte](FITS_HEADER_CARD_SIZE)

      val header = buildHeader(buffer).toArray
      val newOffset = if (data.getPos % FITSBLOCK_SIZE_BYTES == 0) {
        data.getPos
      } else {
        data.getPos + FITSBLOCK_SIZE_BYTES -  data.getPos % FITSBLOCK_SIZE_BYTES
      }

      // Place the cursor at the end of the last header block
      // that is at the beginning of the first data block
      data.seek(newOffset)

      header
    }

    def buildHeader(buffer: Array[Byte], prevline: String="") : List[String] = {
      if (prevline.trim() == "END") {
        Nil
      } else {
        // Read a line of the header
        val len = data.read(buffer, 0, FITS_HEADER_CARD_SIZE)

        // EOF
        val isEmpty = (len <= 0)
        isEmpty match {
          case true => throw new EOFException("nothing to read left")
          case false => isEmpty
        }

        // Decode the line of the header
        val line = new String(buffer, StandardCharsets.UTF_8)

        line :: buildHeader(buffer, line)
      }
    }

    /**
      * Register the header in the Hadoop configuration.
      * By doing this, we broadcast the header to the executors.
      * The header is sent as a long String, and can be read properly
      * afterwards using retrieveHeader. Make sure you use the same
      * separators.
      *
      * @param sep : (String)
      *   Line separator used to form the String. Default is ;;
      *
      */
    def registerHeader(sep : String=";;") {
      conf.set(hdfsPath+"header", blockHeader.mkString(sep))
    }

    /**
      * Retrieve the header from the Hadoop configuration.
      * Make sure you use the same separators as in registerHeader.
      *
      * @param sep : (String)
      *   Line separator used to split the String. Default is ;;
      * @return the header as Array[String]. See readHeader.
      *
      */
    def retrieveHeader(sep : String=";;"): Array[String] = {

      conf.get(hdfsPath+"header").split(sep)
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
      * val myrow = readLineFromBuffer(buffer)
      * }}}
      *
      * @param buf : (Array[Byte])
      *   Row of byte read from the data block.
      * @return (List[_]) The row as list of elements (float, int, string, etc.)
      *   as given by the header.
      *
      */
    def readLineFromBuffer(buf : Array[Byte]): List[_] = {
      if (hdu.implemented) {
        hdu.getRow(buf)
      }
      else null
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
