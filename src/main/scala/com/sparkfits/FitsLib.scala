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

  /**
    * Main class to handle a block of a fits file
    */
  class FitsBlock(hdfsPath : Path, conf : Configuration, hduIndex : Int) {

    // Open the data
    val fs = hdfsPath.getFileSystem(conf)
    val data = fs.open(hdfsPath)

    // Check that the HDU asked is below the max.
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

    val header = readHeader
    resetCursorAtHeader

    val rowTypes = getRowTypes(header)
    val ncols = rowTypes.size

    val splitLocations = (0 :: rowSplitLocations(0)).scan(0)(_ +_).tail

    /**
      * Return the indices of the first and last bytes of the HDU.
      *
      * @return (data_start, data_stop) = (Long, Long, Long), the bytes indices of the HDU.
      *
      */
    def BlockBoundaries : (Long, Long, Long) = {

      // Initialise the file
      data.seek(0)
      var hdu_tmp = 0

      // Initialise the boundaries
      var header_start : Long = 0
      var data_start : Long = 0
      var data_stop : Long = 0

      do {
        // Initialise the offset to the header position
        header_start = data.getPos

        // Get the header (and move after it)
        val header = readHeader

        // Data block starts after the header
        data_start = data.getPos

        // Size of the data block in Bytes.
        // Skip Data if None (typically HDU=0)
        val datalen = Try {
          getNRows(header) * getSizeRowBytes(header)
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

      } while (hdu_tmp < hduIndex + 1 )

      // Reposition the cursor at the beginning of the block
      data.seek(header_start)

      // Return boundaries (included)
      (header_start, data_start, data_stop)
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

      do {

        // Get the header (and move after it)
        // Could be better handled with Try/Success/Failure.
        val header = Try{readHeader}.getOrElse(Array[String]())

        // If the header cannot be read,
        e = if (header.size == 0) {
          false
        } else true

        // Size of the data block in Bytes.
        // Skip Data if None (typically HDU=0)
        val datalen = Try {
          getNRows(header) * getSizeRowBytes(header)
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

      hdu_tmp - 1
    }

    /**
      * Reposition the cursor at the beginning of the block
      */
    def resetCursorAtHeader = {
      // Position the cursor at the beginning of the block
      data.seek(BlockBoundaries._1)
    }

    def resetCursorAtData = {
      // Position the cursor at the beginning of the block
      data.seek(BlockBoundaries._2)
    }

    def setCursor(position : Long) = {
      data.seek(position)
    }

    /**
      * Read a header at a given position
      */
    def readHeader(position : Long) : Array[String] = {
      setCursor(position)
      readHeader
    }

    /**
      * Read the header of the HDU.
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

    // def readLine(col : Int = 0): List[_] = {
    //
    //   // If the cursor is in the header, reposition the cursor at
    //   // the beginning of the data block.
    //   if (data.getPos < blockBoundaries._2) {
    //     resetCursorAtData
    //   }
    //
    //   if (col == ncols) {
    //     Nil
    //   } else {
    //     getElement(rowTypes(col)) :: readLine(col + 1)
    //   }
    // }

    def readLineFromBuffer(buf : Array[Byte], col : Int = 0): List[_] = {

      if (col == ncols) {
        Nil
      } else {
        getElementFromBuffer(buf.slice(splitLocations(col), splitLocations(col+1)), rowTypes(col)) :: readLineFromBuffer(buf, col + 1)
      }
    }

    def getRowTypes(header : Array[String], col : Int = 0): List[String] = {
      val headerNames = getHeaderNames(header)
      val ncols = getNCols(header)
      if (col == ncols) {
        Nil
      } else {
        headerNames("TFORM" + (col + 1).toString) :: getRowTypes(header, col + 1)
      }
    }

    def getHeaderKeys(header : Array[String]) : Array[String] = {
      val keys = new Array[String](header.size)
      val MAX_KEYWORD_LENGTH = 8
      for (i <- 0 to header.size - 1) {
        val line = header(i)
        // Get the key
        keys(i) = line.substring(0, MAX_KEYWORD_LENGTH).trim()
      }
      keys
    }

    def getHeaderValues(header : Array[String]) : HashMap[String, Int] = {
      val headerMap = new HashMap[String, Int]

      val keys = getHeaderKeys(header)
      for (i <- 0 to header.size - 1) {
        val line = header(i)

        // Value - split at the comment
        val v = line.split("/")(0)
        var v_tmp = ""
        var offset = 0
        var letter : Char = 'a'
        do {
          letter = v(29 - offset)
          v_tmp = v_tmp + letter.toString
          offset += 1
        } while (letter != ' ')
        v_tmp = v_tmp.trim().reverse
        headerMap += (keys(i) -> Try{v_tmp.toInt}.getOrElse(0))
      }
      headerMap
    }

    def getHeaderNames(header : Array[String]) : HashMap[String, String] = {
      val headerMap = new HashMap[String, String]
      val MAX_KEYWORD_LENGTH = 8
      val keys = getHeaderKeys(header)
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
      val MAX_KEYWORD_LENGTH = 8
      val keys = getHeaderKeys(header)
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

    // def getElement(fitstype : String) = {
    //
    //   fitstype match {
    //     case "1J" => data.readInt
    //     case "1E" => data.readFloat
    //     case "E" => data.readFloat
    //     case "L" => data.readBoolean
    //     case "D" => data.readDouble
    //     case x if fitstype.endsWith("A") => {
    //       // Example 20A means string on 20 bytes
    //       val buffersize = x.slice(0, x.length - 1).toInt
    //       val buffer = new Array[Byte](buffersize)
    //       data.read(buffer, 0, buffersize)
    //       new String(buffer, StandardCharsets.UTF_8).trim()
    //     }
    //     // case _ => throw new IOError("""Data type not understood!"""")
    //   }
    // }

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
