package com.sparkfits

import nom.tam.fits.{Fits, HeaderCard, Header}
import nom.tam.util.{Cursor}

object SparkFitsUtil {
  /**
    * Get the number of HDUs
    */
  def getNHdus(f : Fits, n : Int = 0) : Int = {
    if (f.getHDU(n) != null) getNHdus(f, n + 1) else n
  }

  /**
    * Get the header
    */
  def getMyHeader(c : Cursor[String, HeaderCard], s : String) : String = {
    if (c.hasNext() == true) getMyHeader(c, s + c.next() + ",") else s
  }
}
