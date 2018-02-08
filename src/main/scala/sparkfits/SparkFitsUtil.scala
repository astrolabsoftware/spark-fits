package sparkfits

import nom.tam.fits.{Fits, HeaderCard, Header}
import nom.tam.util.{Cursor}

class SparkFitsUtil {

  /** Get the number of HDUs */
  def getNHdus(f : Fits, n : Int) : Int = {
    if (f.getHDU(n) != null) getNHdus(f, n + 1) else n
  }

  /** Get the header */
  def getMyHeader(c : Cursor[String, HeaderCard], s : String) : String = {
    if (c.hasNext() == true) getMyHeader(c, s + c.next() + ",") else s
  }

  /** Another method to get the header */
  def header(h : Header) = {
    val c = h.iterator()
    do {
      val card = c.next()
      val key = card.getKey
      try {
        val valueType = card.valueType
        // println("===key", key)
        val typ = key match {
          case "END" => ""
          case _ => card.valueType.getCanonicalName
        }
        val value = key match {
          case "END" => ""
          case _ => card.getValue.toString
        }
        // println(s"  key=$key type=$typ value=$value")
      } catch {
        case e:Exception =>
      }
    } while (c.hasNext)
  }
}
