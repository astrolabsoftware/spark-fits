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

import java.io.IOError
import java.nio.ByteBuffer
import java.io.EOFException
import java.nio.charset.StandardCharsets

import scala.collection.mutable.HashMap

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration

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
    val numberOfHdus = if (conf.get(hdfsPath+"_header") == null) {
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
    val blockBoundaries = if (conf.get(hdfsPath+"_blockboundaries") != null) {
      retrieveBlockBoundaries()
    } else BlockBoundaries

    val empty_hdu = if (blockBoundaries._2 == blockBoundaries._3) {
      true
    } else false

    // Get the header and set the cursor to its start.
    val blockHeader = if (conf.get(hdfsPath+"header") != null) {
      retrieveHeader()
    } else readHeader
    resetCursorAtHeader

    // Get informations on element types and number of columns.
    val rowTypes = if (empty_hdu) {
      List[String]()
    } else getColTypes(blockHeader)
    val ncols = rowTypes.size

    // Check if the user specifies columns to select
    val colNames = getHeaderNames(blockHeader)

    val selectedColNames = if (conf.get("columns") != null) {
      conf.getStrings("columns").deep.toList.asInstanceOf[List[String]]
    } else {
      colNames.filter(x=>x._1.contains("TYPE")).values.toList.asInstanceOf[List[String]]
    }
    val colPositions = selectedColNames.map(
      x=>getColumnPos(blockHeader, x)).toList.sorted

    // splitLocations is an array containing the location of elements
    // (byte index) in a row. Example if we have a row with [20A, E, E], one
    // will have splitLocations = [0, 20, 24, 28] that is a string on 20 Bytes,
    // followed by 2 floats on 4 bytes each.
    val splitLocations = if (empty_hdu) {
      List[Int]()
    } else {
      (0 :: rowSplitLocations(0)).scan(0)(_ +_).tail
    }

    /**
      * Check the type of HDU. Available: BINTABLE, IMAGE, or EMPTY.
      * If not registered, returns NOT UNDERSTOOD.
      * Note: Not working if an image is stored in a primary HDU... TBD.
      *
      * @return (String) The type of the HDU data.
      */
    def hduType : String = {
      // Get the header NAMES
      val colNames = getHeaderNames(blockHeader)

      // Check if the HDU is empty, a table or an image
      val isTable = colNames.filter(
        x=>x._2.contains("BINTABLE")).values.toList.size > 0
      val isImage = colNames.filter(
        x=>x._2.contains("IMAGE")).values.toList.size > 0
      val isEmpty = empty_hdu

      val fitstype = if (isTable) {
        "BINTABLE"
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
      * Return the indices of the first and last bytes of the HDU:
      * hdu_start=header_start, data_start, data_stop, hdu_stop
      *
      * @return (Long, Long, Long, Long), the split of the HDU.
      *
      */
    def BlockBoundaries : (Long, Long, Long, Long) = {

      // Initialise the cursor position at the beginning of the file
      data.seek(0)
      var hdu_tmp = 0

      // Initialise the boundaries
      var header_start : Long = 0
      var data_start : Long = 0
      var data_stop : Long = 0
      var block_stop : Long = 0

      // Loop over HDUs, and stop at the desired one.
      do {
        // Initialise the offset to the header position
        header_start = data.getPos

        // add the header size (and move after it)
        val localHeader = readHeader

        // Data block starts after the header
        data_start = data.getPos

        // Size of the data block in Bytes.
        // Skip Data if None (typically HDU=0)
        val datalen = Try {
          getNRows(localHeader) * getSizeRowBytes(localHeader).toLong
        }.getOrElse(0L)

        // Where the actual data stopped
        data_stop = data.getPos + datalen

        // Store the final offset
        // FITS is made of blocks of size 2880 bytes, so we might need to
        // pad to jump from the end of the data to the next header.
        block_stop = if ((data.getPos + datalen) % FITSBLOCK_SIZE_BYTES == 0) {
          data_stop
        } else {
          data_stop + FITSBLOCK_SIZE_BYTES -  (data_stop) % FITSBLOCK_SIZE_BYTES
        }

        // Move to the another HDU if needed
        hdu_tmp = hdu_tmp + 1
        data.seek(block_stop)

      } while (hdu_tmp < hduIndex + 1 )

      // Reposition the cursor at the beginning of the block
      data.seek(header_start)

      // Return boundaries (included):
      // hdu_start=header_start, data_start, data_stop, hdu_stop
      (header_start, data_start, data_stop, block_stop)
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
      var hdu_tmp = 0

      // Initialise the boundaries
      var data_stop : Long = 0
      var e : Boolean = true

      // Loop over all HDU, and exit.
      do {

        // Get the header (and move after it)
        // Could be better handled with Try/Success/Failure.
        val localHeader = Try{readHeader}.getOrElse(Array[String]())

        // If the header cannot be read,
        e = if (localHeader.size == 0) {
          false
        } else true

        // Size of the data block in Bytes.
        // Skip Data if None (typically HDU=0)
        val datalen = Try {
          getNRows(localHeader) * getSizeRowBytes(localHeader).toLong
        }.getOrElse(0L)

        // Store the final offset
        // FITS is made of blocks of size 2880 bytes, so we might need to
        // pad to jump from the end of the data to the next header.
        data_stop = if ((data.getPos + datalen) % FITSBLOCK_SIZE_BYTES == 0) {
          data.getPos + datalen
        } else {
          data.getPos + datalen + FITSBLOCK_SIZE_BYTES -  (data.getPos + datalen) % FITSBLOCK_SIZE_BYTES
        }

        // Move to the another HDU if needed
        hdu_tmp = hdu_tmp + 1
        data.seek(data_stop)

      } while (e)

      // Return the number of HDU.
      hdu_tmp - 1
    }

    /**
      * Place the cursor at the beginning of the header of the block
      *
      */
    def resetCursorAtHeader = {
      // Place the cursor at the beginning of the block
      data.seek(blockBoundaries._1)
    }

    /**
      * Place the cursor at the beginning of the data of the block
      *
      */
    def resetCursorAtData = {
      // Place the cursor at the beginning of the block
      data.seek(blockBoundaries._2)
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
      *   header position. Use in combination with BlockBoundaries._1
      *   for example.
      * @return (Array[String) the header is an array of Strings, each String
      *   being one line of the header.
      */
    def readHeader(position : Long) : Array[String] = {
      setCursor(position)
      readHeader
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
      * Register the boundaries of the HDU in the Hadoop configuration.
      * By doing this, we broadcast the values to the executors.
      * It is sent as a long String, and can be read properly
      * afterwards using retrieveBlockBoundaries. Make sure you use the same
      * separators.
      *
      * @param sep : (String)
      *   Line separator used to form the String. Default is ;;
      *
      */
    def registerBlockBoundaries(sep : String=";;") {
      // Register the Tuple4 as a String.. Ugly
      val str = blockBoundaries.productIterator.toArray.mkString(sep)

      conf.set(hdfsPath+"blockboundaries", str)
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
      * Retrieve the blockboundaries from the Hadoop configuration.
      * Make sure you use the same separators as in registerBlockBoundaries.
      *
      * @param sep : (String)
      *   Line separator used to split the String. Default is ;;
      * @return the block boundaries as Tuple4 of Long. See BlockBoundaries.
      *
      */
    def retrieveBlockBoundaries(sep : String=";;"): (Long, Long, Long, Long) = {
      // Retrieve the boundaries as a String, split it, and cast to Long
      val arr = conf.get(hdfsPath+"blockboundaries").split(sep).map(x => x.toLong)

      // Return it as a tuple4 of Long... Ugly... Need to change that!
      (arr(0), arr(1), arr(2), arr(3))
    }

    /**
      * Convert binary row into row. You need to have the cursor at the
      * beginning of a row. Example
      * {{{
      * // Set the cursor at the beginning of the data block
      * setCursor(BlockBoundaries._2)
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

      val row = List.newBuilder[Any]
      for (col <- colPositions) {
        row += getElementFromBuffer(
          buf.slice(splitLocations(col), splitLocations(col+1)), rowTypes(col))
      }
      row.result
    }

    /**
      * Companion to readLineFromBuffer. Convert one array of bytes
      * corresponding to one element of the table into its primitive type.
      *
      * @param subbuf : (Array[Byte])
      *   Array of byte describing one element of the table.
      * @param fitstype : (String)
      *   The type of this table element according to the header.
      * @return the table element converted from binary.
      *
      */
    def getElementFromBuffer(subbuf : Array[Byte], fitstype : String) : Any = {
      fitstype match {
        // 16-bit Integer
        case x if fitstype.contains("I") => {
          ByteBuffer.wrap(subbuf, 0, 2).getShort()
        }
        // 32-bit Integer
        case x if fitstype.contains("J") => {
          ByteBuffer.wrap(subbuf, 0, 4).getInt()
        }
        // 64-bit Integer
        case x if fitstype.contains("K") => {
          ByteBuffer.wrap(subbuf, 0, 8).getLong()
        }
        // Single precision floating-point
        case x if fitstype.contains("E") => {
          ByteBuffer.wrap(subbuf, 0, 4).getFloat()
        }
        // Double precision floating-point
        case x if fitstype.contains("D") => {
          ByteBuffer.wrap(subbuf, 0, 8).getDouble()
        }
        // Boolean
        case x if fitstype.contains("L") => {
          // 1 Byte containing the ASCII char T(rue) or F(alse).
          subbuf(0).toChar == 'T'
        }
        // Chain of characters
        case x if fitstype.endsWith("A") => {
          // Example 20A means string on 20 bytes
          new String(subbuf, StandardCharsets.UTF_8).trim()
        }
        case _ => {
          println(s"""
              Cannot infer size of type $fitstype from the header!
              See com.sparkfits.FitsLib.getElementFromBuffer
              """)
          0
        }
      }
    }

    /**
      * Return the types of elements for each column as a list.
      *
      * @param col : (Int)
      *   Column index used for the recursion.
      * @return (List[String]), list with the types of elements for each column
      *   as given by the header.
      *
      */
    def getColTypes(header : Array[String], col : Int = 0): List[String] = {
      // Get the names of the Columns
      val headerNames = getHeaderNames(header)

      // Get the number of Columns by recursion
      val ncols = getNCols(header)
      if (col == ncols) {
        Nil
      } else {
        headerNames("TFORM" + (col + 1).toString) :: getColTypes(header, col + 1)
      }
    }

    /**
      * Return the KEYWORDS of the header.
      *
      * @return (Array[String]), array with the KEYWORDS of the HDU header.
      *
      */
    def getHeaderKeywords(header : Array[String]) : Array[String] = {
      // Get the KEYWORDS
      val keywords = new Array[String](header.size)

      // Loop over KEYWORDS
      for (i <- 0 to header.size - 1) {
        val line = header(i)
        // Get the keyword
        keywords(i) = line.substring(0, MAX_KEYWORD_LENGTH).trim()
      }
      keywords
    }

    /**
      * Return the (KEYWORDS, VALUES) of the header
      *
      * @return (HashMap[String, Int]), map array with (keys, values_as_int).
      *
      */
    def getHeaderValues(header : Array[String]) : HashMap[String, Int] = {

      // Initialise our map
      val headerMap = new HashMap[String, Int]

      // Get the KEYWORDS of the Header
      val keys = getHeaderKeywords(header)

      // Loop over rows
      for (i <- 0 to header.size - 1) {

        // One row
        val row = header(i)

        // Split at the comment
        val v = row.split("/")(0)

        // Init
        var v_tmp = ""
        var offset = 0
        var letter : Char = 'a'

        // recursion to get the value. Reverse order!
        // 29. WTF???
        do {
          letter = v(29 - offset)
          v_tmp = v_tmp + letter.toString
          offset += 1
        } while (letter != ' ')

        // Reverse our result, and look for Int value.
        // Could be better... Especially if we have something else than Int?
        v_tmp = v_tmp.trim().reverse
        headerMap += (keys(i) -> Try{v_tmp.toInt}.getOrElse(0))
      }
      // Return the map(KEYWORDS -> VALUES)
      headerMap
    }

    /**
      * Get the names of the header.
      * We assume that the names are inside quotes 'my_name'.
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @return (HashMap[String, String]), a map of keyword/name.
      *
      */
    def getHeaderNames(header : Array[String]) : HashMap[String, String] = {

      // Initialise the map
      val headerMap = new HashMap[String, String]

      // Get the KEYWORDS
      val keys = getHeaderKeywords(header)
      for (i <- 0 to header.size - 1) {

        // Take one row and make it an iterator of Char
        // from the end of the KEYWORD.
        val row = header(i)
        val it = row.substring(MAX_KEYWORD_LENGTH, FITS_HEADER_CARD_SIZE).iterator

        var name_tmp = ""
        var isName = false

        // Loop over the Chars of the row
        do {
          val nextChar = it.next()

          // Trigger/shut name completion
          if (nextChar == ''') {
            isName = !isName
          }

          // Add what is inside the quotes (left quote included)
          if (isName) {
            name_tmp = name_tmp + nextChar
          }
        } while (it.hasNext)

        // Try to see if there is something inside quotes
        // Return empty String otherwise.
        val name = Try{name_tmp.substring(1, name_tmp.length).trim()}.getOrElse("")

        // Update the map
        if (name != "") {
          headerMap += (keys(i) -> name)
        }
      }

      // Return the map
      headerMap
    }

    /**
      * Get the comments of the header.
      * We assume the comments are written after a backslash (\).
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @return (HashMap[String, String]), a map of keyword/comment.
      *
      */
    def getHeaderComments(header : Array[String]) : HashMap[String, String] = {

      // Init
      val headerMap = new HashMap[String, String]

      // Get the KEYWORDS
      val keys = getHeaderKeywords(header)

      // Loop over header row
      for (i <- 0 to header.size - 1) {
        // One row
        val row = header(i)

        // comments are written after a backslash (\).
        // If None, return empty String.
        val comments = Try{row.split("/")(1).trim()}.getOrElse("")
        headerMap += (keys(i) -> comments)
      }

      // Return the Map.
      headerMap
    }

    /**
      * Get the number of row of a HDU.
      * We rely on what's written in the header, meaning
      * here we do not access the data directly.
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @return (Long), the number of rows as written in KEYWORD=NAXIS2.
      *
      */
    def getNRows(header : Array[String]) : Long = {
      val values = getHeaderValues(header)
      values("NAXIS2")
    }

    /**
      * Get the number of column of a HDU.
      * We rely on what's written in the header, meaning
      * here we do not access the data directly.
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @return (Long), the number of rows as written in KEYWORD=TFIELDS.
      *
      */
    def getNCols(header : Array[String]) : Long = {
      val values = getHeaderValues(header)
      values("TFIELDS")
    }

    /**
      * Get the size (bytes) of each row of a HDU.
      * We rely on what's written in the header, meaning
      * here we do not access the data directly.
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @return (Int), the size (bytes) of one row as written in KEYWORD=NAXIS1.
      *
      */
    def getSizeRowBytes(header : Array[String]) : Int = {
      val values = getHeaderValues(header)
      values("NAXIS1")
    }

    /**
      * Get the name of a column with index `colIndex` of a HDU.
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @param colIndex : (Int)
      *   Index (zero-based) of a column.
      * @return (String), the name of the column.
      *
      */
    def getColumnName(header : Array[String], colIndex : Int) : String = {
      // Grab the header names as map(keywords/names)
      val names = getHeaderNames(header)
      // Zero-based index
      names("TTYPE" + (colIndex + 1).toString)
    }

    /**
      * Get the position (zero based) of a column with name `colName` of a HDU.
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @param colName : (String)
      *   The name of the column
      * @return (Int), position (zero-based) of the column.
      *
      */
    def getColumnPos(header : Array[String], colName : String) : Int = {
      // Grab the header names as map(keywords/names)
      val names = getHeaderNames(header)

      // Get the position of the column. Header names are TTYPE#
      val pos = Try {
        names.filter(x => x._2.toLowerCase == colName.toLowerCase).keys.head.substring(5).toInt
      }.getOrElse(-1)

      val isCol = pos >= 0
      isCol match {
        case true => isCol
        case false => throw new AssertionError(s"""
          $colName is not a valid column name!
          """)
      }

      // Zero based
      pos - 1
    }

    /**
      * Get the type of the elements of a column with index `colIndex` of a HDU.
      *
      * @param header : (Array[String])
      *   The header of the HDU.
      * @param colIndex : (Int)
      *   Index (zero-based) of a column.
      * @return (String), the type (FITS convention) of the elements of the column.
      *
      */
    def getColumnType(header : Array[String], colIndex : Int) : String = {
      // Grab the header names as map(keywords/names)
      val names = getHeaderNames(header)
      // Zero-based index
      names("TFORM" + (colIndex + 1).toString)
    }

    /**
      * Description of a row in terms of bytes indices.
      * rowSplitLocations returns an array containing the position of elements
      * (byte index) in a row. Example if we have a row with [20A, E, E], one
      * will have rowSplitLocations -> [0, 20, 24, 28] that is a string
      * on 20 Bytes, followed by 2 floats on 4 bytes each.
      *
      * @param col : (Int)
      *   Column position used for the recursion. Should be left at 0.
      * @return (List[Int]), the position of elements (byte index) in a row.
      *
      */
    def rowSplitLocations(col : Int = 0) : List[Int] = {
      if (col == ncols) {
        Nil
      } else {
        getSplitLocation(rowTypes(col)) :: rowSplitLocations(col + 1)
      }
    }

    /**
      * Companion routine to rowSplitLocations. Returns the size of a primitive
      * according to its type from the FITS header.
      *
      * @param fitstype : (String)
      *   Element type according to FITS standards (I, J, K, E, D, L, A, etc)
      * @return (Int), the size (bytes) of the element.
      *
      */
    def getSplitLocation(fitstype : String) : Int = {
      fitstype match {
        case x if fitstype.contains("I") => 2
        case x if fitstype.contains("J") => 4
        case x if fitstype.contains("K") => 8
        case x if fitstype.contains("E") => 4
        case x if fitstype.contains("D") => 8
        case x if fitstype.contains("L") => 1
        case x if fitstype.endsWith("A") => {
          // Example 20A means string on 20 bytes
          x.slice(0, x.length - 1).toInt
        }
        case _ => {
          println(s"""
              Cannot infer size of type $fitstype from the header!
              See com.sparkfits.FitsLib.getSplitLocation
              """)
          0
        }
      }
    }
  }
}
