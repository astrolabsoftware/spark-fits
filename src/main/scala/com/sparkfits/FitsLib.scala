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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

import scala.util.{Try, Success, Failure}

object FitsLib {

  // Standard size of a header (bytes)
  val HEADER_SIZE_BYTES = 2880

  // Size of one row in header (bytes)
  val FITS_HEADER_CARD_SIZE = 80

  // Size of KEYWORD (KEYS) in FITS (bytes)
  val MAX_KEYWORD_LENGTH = 8

  /**
    * Main class to handle a block of a fits file. Main features are
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
    val numberOfHdus = getNHDU
    val isHDUBelowMax = hduIndex < numberOfHdus
    isHDUBelowMax match {
      case true => isHDUBelowMax
      case false => throw new AssertionError(s"""
        HDU number $hduIndex does not exist!
        """)
    }

    // Compute the bound and initialise the cursor
    // indices (headerStart, dataStart, dataStop) in bytes.
    val blockBoundaries = BlockBoundaries
    // println(blockBoundaries)

    // Get the header and set the cursor to its start.
    val blockHeader = readHeader
    resetCursorAtHeader

    // Get informations on element types and number of columns.
    val rowTypes = getColTypes(blockHeader)
    val ncols = rowTypes.size

    // splitLocations is an array containing the location of elements
    // (byte index) in a row. Example if we have a row with [20A, E, E], one
    // will have splitLocations = [0, 20, 24, 28] that is a string on 20 Bytes,
    // followed by 2 floats on 4 bytes each.
    val splitLocations = (0 :: rowSplitLocations(0)).scan(0)(_ +_).tail

    /**
      * Return the indices of the first and last bytes of the HDU.
      *
      * @return (header_start, data_start, data_stop, block_stop) = (Long, Long, Long, Long),
      *   the bytes indices of the HDU.
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
        // data.seek(header_start + HEADER_SIZE_BYTES)
        val localHeader = readHeader

        // Data block starts after the header
        data_start = data.getPos

        // Size of the data block in Bytes.
        // Skip Data if None (typically HDU=0)
        val datalen = Try {
          getNRows(localHeader) * getSizeRowBytes(localHeader)
        }.getOrElse(0L)

        // Where the actual data stopped
        data_stop = data.getPos + datalen

        // Store the final offset
        // FITS is made of blocks of size 2880 bytes, so we might need to
        // pad to jump from the end of the data to the next header.
        block_stop = if ((data.getPos + datalen) % HEADER_SIZE_BYTES == 0) {
          data_stop
        } else {
          data_stop + HEADER_SIZE_BYTES -  (data_stop) % HEADER_SIZE_BYTES
        }

        // Move to the another HDU if needed
        hdu_tmp = hdu_tmp + 1
        data.seek(block_stop)

      } while (hdu_tmp < hduIndex + 1 )

      // Reposition the cursor at the beginning of the block
      data.seek(header_start)

      // Return boundaries (included)
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
          getNRows(localHeader) * getSizeRowBytes(localHeader)
        }.getOrElse(0L)

        // Store the final offset
        // FITS is made of blocks of size 2880 bytes, so we might need to
        // pad to jump from the end of the data to the next header.
        data_stop = if ((data.getPos + datalen) % HEADER_SIZE_BYTES == 0) {
          data.getPos + datalen
        } else {
          data.getPos + datalen + HEADER_SIZE_BYTES -  (data.getPos + datalen) % HEADER_SIZE_BYTES
        }

        // Move to the another HDU if needed
        hdu_tmp = hdu_tmp + 1
        data.seek(data_stop)

      } while (e)

      // Return the number of HDU.
      hdu_tmp - 1
    }

    /**
      * Reposition the cursor at the beginning of the header of the block
      *
      */
    def resetCursorAtHeader = {
      // Position the cursor at the beginning of the block
      data.seek(blockBoundaries._1)
    }

    /**
      * Reposition the cursor at the beginning of the data of the block
      *
      */
    def resetCursorAtData = {
      // Position the cursor at the beginning of the block
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
      * the header.
      *
      * @return (Array[String) the header is an array of Strings, each String
      *   being one line of the header.
      */
    def readHeader : Array[String] = {

      // Initialise a line of the header
      var buffer = new Array[Byte](FITS_HEADER_CARD_SIZE)

      var len = 0
      var stop = 0
      var pos = 0
      var stopline = 0
      var header = new Array[String](HEADER_SIZE_BYTES / FITS_HEADER_CARD_SIZE)

      // Loop until the end of the header.
      // TODO: what if the header has an non-standard size?
      do {
        len = data.read(buffer, 0, FITS_HEADER_CARD_SIZE)
        if (len == 0) {
          throw new EOFException("nothing to read left")
        }
        stop += len

        // Bytes to Strings
        header(pos) = new String(buffer, StandardCharsets.UTF_8)

        // Remove blanck lines at the end
        stopline = if (header(pos).trim() != "") {
          stopline + 1
        } else stopline

        // Increment the line
        pos += 1
      } while (stop < HEADER_SIZE_BYTES)

      // Return the header
      header.slice(0, stopline)
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
      * @param col : (Int=0)
      *   Index of the column (used for the recursion).
      * @return (List[_]) The row as list of elements (float, int, string, etc.)
      *   as given by the header.
      *
      */
    def readLineFromBuffer(buf : Array[Byte], col : Int = 0): List[_] = {

      if (col == ncols) {
        Nil
      } else {
        getElementFromBuffer(buf.slice(splitLocations(col), splitLocations(col+1)), rowTypes(col)) :: readLineFromBuffer(buf, col + 1)
      }
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
        case "1J" => {
          ByteBuffer.wrap(subbuf, 0, 4).getInt()
        }
        // case "1E" => {
        //   ByteBuffer.wrap(subbuf, 0, 4).getFloat()
        // }
        case x if fitstype.contains("E") => {
          ByteBuffer.wrap(subbuf, 0, 4).getFloat()
        }
        // case "E" => {
        //   ByteBuffer.wrap(subbuf, 0, 4).getFloat()
        // }
        case "L" => {
          // 1 Byte containing the ASCII char T(rue) or F(alse).
          subbuf(0).toChar == 'T'
        }
        case "D" => {
          ByteBuffer.wrap(subbuf, 0, 8).getDouble()
        }
        case x if fitstype.endsWith("A") => {
          // Example 20A means string on 20 bytes
          new String(subbuf, StandardCharsets.UTF_8).trim()
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

      // Get the number of Columns
      val ncols = getNCols(header)
      if (col == ncols) {
        Nil
      } else {
        headerNames("TFORM" + (col + 1).toString) :: getColTypes(header, col + 1)
      }
    }

    /**
      * Return the KEYS of the header.
      *
      * @return (Array[String]), array with the KEYS of the HDU header.
      *
      */
    def getHeaderKeywords(header : Array[String]) : Array[String] = {
      // Get the KEYWORDS
      val keywords = new Array[String](header.size)

      // Loop over KEYWORDS
      for (i <- 0 to header.size - 1) {
        val line = header(i)
        // Get the key
        keywords(i) = line.substring(0, MAX_KEYWORD_LENGTH).trim()
      }
      keywords
    }

    /**
      * Return the (KEYS, VALUES) of the header
      *
      * @return (HashMap[String, Int]), map array with (keys, values_as_int).
      *
      */
    def getHeaderValues(header : Array[String]) : HashMap[String, Int] = {

      // Initialise our map
      val headerMap = new HashMap[String, Int]

      // Get the KEYS of the Header
      val keys = getHeaderKeywords(header)

      // Loop over KEYS
      for (i <- 0 to header.size - 1) {

        // One line
        val line = header(i)

        // Split at the comment
        val v = line.split("/")(0)

        // Init
        var v_tmp = ""
        var offset = 0
        var letter : Char = 'a'

        // recursion to get the value. Reverse order!
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
      // Return the map(KEYS -> VALUES)
      headerMap
    }

    def getHeaderNames(header : Array[String]) : HashMap[String, String] = {
      val headerMap = new HashMap[String, String]
      val keys = getHeaderKeywords(header)
      for (i <- 0 to header.size - 1) {
        val line = header(i)

        val it = line.substring(MAX_KEYWORD_LENGTH, 80).iterator

        // You are supposed to see
        //  - the name or nothing
        //  - the value or nothing
        //  - the comment or nothing
        var name_tmp = ""
        var val_tmp = ""
        var isName = false

        do {
          val nextChar = it.next()

          // Trigger/shut name completion
          if (nextChar == ''') {
            isName = !isName
          }

          // Complete the name
          if (isName) {
            name_tmp = name_tmp + nextChar
          }
        } while (it.hasNext)

        val name = Try{name_tmp.substring(1, name_tmp.length).trim()}.getOrElse("")
        headerMap += (keys(i) -> name)
      }
      headerMap
    }

    def getHeaderComments(header : Array[String]) : HashMap[String, String] = {
      val headerMap = new HashMap[String, String]
      val keys = getHeaderKeywords(header)
      for (i <- 0 to header.size - 1) {
        val line = header(i)

        val comments = Try{line.split("/")(1).trim()}.getOrElse("")
        headerMap += (keys(i) -> comments)
      }
      headerMap
    }

    def getNRows(header : Array[String]) : Long = {
      val values = getHeaderValues(header)
      values("NAXIS2")
    }

    def getNCols(header : Array[String]) : Long = {
      val values = getHeaderValues(header)
      values("TFIELDS")
    }

    def getSizeRowBytes(header : Array[String]) : Long = {
      val values = getHeaderValues(header)
      values("NAXIS1")
    }

    def getColumnName(header : Array[String], colIndex : Int) : String = {
      val names = getHeaderNames(header)
      // zero-based index
      names("TTYPE" + (colIndex + 1).toString)
    }

    def getColumnType(header : Array[String], colIndex : Int) : String = {
      val names = getHeaderNames(header)
      // zero-based index
      names("TFORM" + (colIndex + 1).toString)
    }

    def rowSplitLocations(col : Int = 0) : List[Int] = {
      if (col == ncols) {
        Nil
      } else {
        getSplitLocation(rowTypes(col)) :: rowSplitLocations(col + 1)
      }
    }

    def getSplitLocation(fitstype : String) : Int = {
      fitstype match {
        case "1J" => 4
        case "1E" => 4
        case "E" => 4
        case "B" => 4
        case "L" => 1
        case "D" => 8
        case x if fitstype.endsWith("A") => {
          // Example 20A means string on 20 bytes
          x.slice(0, x.length - 1).toInt
        }
      }
    }
  }
}
