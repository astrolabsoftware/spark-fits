package sparkfits

import org.apache.spark.sql.{SparkSession, DataFrame, SQLContext, Row}
import org.apache.spark.sql.types._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.rdd.RDD

import org.apache.log4j.Level
import org.apache.log4j.Logger

import nom.tam.fits.{Fits, HeaderCard, BinaryTableHDU, Header, BinaryTable}
import nom.tam.util.{Cursor, BufferedFile, ArrayDataInput}

object proc {
  def get(b : BinaryTableHDU, row : Int) : Seq[Double] = {
    // b.getElement(col, row).asInstanceOf[Array[Double]](0)
    b.getRow(row).seq.asInstanceOf[Seq[Array[Double]]].flatMap(x=>x)
  }
}

// case class proc(b : BinaryTableHDU, col : Int, row : Int) extends java.io.Serializable {
//     b.getElement(col, row).asInstanceOf[Array[Double]](0)
// }

object ReadFits {

  // Set to Level.WARN is you want verbosity
  // Logger.getLogger("org").setLevel(Level.OFF)
  // Logger.getLogger("akka").setLevel(Level.OFF)

  // Initialise Spark context
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val sq = new SQLContext(sc)
  val spark = SparkSession
    .builder()
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  // Get the number of HDUs
  def getNHdus(f : Fits, n : Int) : Int = {
    if (f.getHDU(n) != null) getNHdus(f, n + 1) else n
  }

  // Get the header
  def getMyHeader(c : Cursor[String, HeaderCard], s : String) : String = {
    if (c.hasNext() == true) getMyHeader(c, s + c.next() + ",") else s
  }

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

  def main(args : Array[String]) {
    // Open the fits
    val ffits = new Fits(args(0).toString) with Serializable
    // ffits.read()

    // Get the total number of HDUs
    // val nHDUs = ffits.getNumberOfHDUs()
    val nHDUs = getNHdus(ffits, 0)

    println("Number of HDUs : " + nHDUs.toString)

    // Process only HDU > 0
    for (pos <- 1 to nHDUs - 1) {
      println(s"Processing HDU number $pos")

      // Get the hdu
      val hdu = ffits.getHDU(pos)

      // Check it is a table
      val isTable = hdu.isInstanceOf[BinaryTableHDU]
      if (isTable == false) {
        System.err.println("Work only with BinaryTableHDU!")
        System.exit(1)
      }

      val data = hdu.asInstanceOf[BinaryTableHDU]

      val ncols = data.getNCols
      val nrows = data.getNRows
      println(s"Table with [col x row] = [$ncols x $nrows]")

      val nBlock = 100
      val sizeBlock : Int = nrows / nBlock
      val nParts = 100
      val col0 = 0

      val it = data.getHeader.iterator
      val myheader = getMyHeader(it, "")

      println(myheader)

      println(data.getColumnName(0))
      println(data.getColumnName(1))
      println(data.getColumnFormat(0))
      println(data.getColumnFormat(1))
      val fitsSchema = StructType(
        List(
          StructField("ra", DoubleType, false),
          StructField("dec", DoubleType, false)
        )
      )

      def yieldRows(x : Fits, col : Int, offset : Int, sizeBlock : Int) = {

        // Start of the block
        val start = offset * sizeBlock

        // End of the block
        val stop_tmp = (offset + 1) * sizeBlock - 1
        val stop = if (stop_tmp < nrows - 1) {
          stop_tmp
        } else {
          nrows - 1
        }

        // Yield rows
        for {
          i <- start to stop
        } yield (x.getHDU(1)
          .asInstanceOf[BinaryTableHDU]
          .getElement(i, col)
          .asInstanceOf[Array[_]](0)
        )
      }

      def yieldRows2(x : Fits, col : Int, offset : Int, sizeBlock : Int) = {

        // Start of the block
        val start = offset * sizeBlock

        // End of the block
        val stop_tmp = (offset + 1) * sizeBlock - 1
        val stop = if (stop_tmp < nrows - 1) {
          stop_tmp
        } else {
          nrows - 1
        }

        // Yield rows
        for {
          i <- start to stop
        } yield (x.getHDU(1)
          .asInstanceOf[BinaryTableHDU]
          .getRow(i)
          .map(x=>x.asInstanceOf[Array[Double]](0))
        )
      }

      def yieldRows3(x : Fits, col : Int, offset : Int, sizeBlock : Int) = {

        // Start of the block
        val start = offset * sizeBlock

        // End of the block
        val stop_tmp = (offset + 1) * sizeBlock - 1
        val stop = if (stop_tmp < nrows - 1) {
          stop_tmp
        } else {
          nrows - 1
        }

        // Yield rows
        for {
          i <- start to stop
        } yield (Array(x.getHDU(1)
          .asInstanceOf[BinaryTableHDU]
          .getRow(i).map {
            case x : Array[_] => x.asInstanceOf[Array[_]](0)
            case x : String => x
          })
          .map{case x => (x(0).toString, x(1).toString, x(2).toString)}.toList(0)
        )
      }

      def yieldEmptyRows(offset : Int, sizeBlock : Int) = {

        // Start of the block
        val start = offset * sizeBlock

        // End of the block
        val stop_tmp = (offset + 1) * sizeBlock - 1
        val stop = if (stop_tmp < nrows - 1) {
          stop_tmp
        } else {
          nrows - 1
        }

        // Yield rows
        for {
          i <- start to stop
        } yield (i)
      }

      def getDF(rdd : RDD[(Any, Int)], fitstype : String, name : String) = {
        val x = fitstype match {
          case "1J" => rdd.asInstanceOf[RDD[(Int, Int)]].toDF(name, "id")
          case "1E" => rdd.asInstanceOf[RDD[(Double, Int)]].toDF(name, "id")
          case "D" => rdd.asInstanceOf[RDD[(Double, Int)]].toDF(name, "id")
          case _ => rdd.asInstanceOf[RDD[(Float, Int)]].toDF(name, "id")
        }
        x
      }

      def recurDF(df : DataFrame, col : Int, colmax : Int) : DataFrame = {
        if (col == colmax + 1) {
          df
        } else {
          val rdd_tmp = sc.parallelize(0 to nBlock - 1, nParts)
            .map(blockid => (blockid, new Fits(args(0).toString) with Serializable ))
            .map(x => yieldRows(x._2, col, x._1, sizeBlock).zip(yieldEmptyRows(x._1, sizeBlock)))
            .flatMap(x => x)

          val fitstype : String = data.getColumnFormat(col)
          val df_tmp = getDF(rdd_tmp, fitstype, col.toString)

          println(df_tmp.count())
          println(df.join(df_tmp, "id").count())
          recurDF(df.join(df_tmp, "id"), col + 1, colmax)
        }
      }
      // def recurDF(df : DataFrame, col : Int, colmax : Int) : DataFrame = {
      //   if (col == colmax + 1) {
      //     df
      //   } else {
      //     val rdd_tmp = sc.parallelize(0 to nBlock - 1, nParts)
      //       .map(blockid => (blockid, new Fits(args(0).toString) with Serializable ))
      //       .map(x => yieldRows(x._2, col, x._1, sizeBlock))
      //       .flatMap(x => x)
      //       .map(x => x.toString)
      //
      //     recurDF(df.rdd.map(x=>x(0).toString).zip(rdd_tmp).toDF, col + 1, colmax)
      //   }
      // }


      // Initialisation (1st column)
      // val rdd = sc.parallelize(0 to nBlock - 1, nParts)
      //   .map(blockid => (blockid, new Fits(args(0).toString) with Serializable ))
      //   .map(x => yieldRows(x._2, col0, x._1, sizeBlock).zip(yieldEmptyRows(x._1, sizeBlock)))
      //   .flatMap(x => x)
      val rdd = sc.parallelize(0 to nBlock - 1, nParts)
        .map(blockid => (blockid, new Fits(args(0).toString) with Serializable ))
        .map(x => yieldRows3(x._2, col0, x._1, sizeBlock))
        .flatMap(x => x)

      // println(rdd.count())
      // rdd.take(10)
      val df = rdd.toDF()
      df.show()
      df.printSchema()
      // Get the correct type for the column
      // Type of the data
      // val fitstype : String = data.getColumnFormat(col0)
      // val df = getDF(rdd, fitstype, col0.toString)
      // val rddtyped = getRDDType(rdd, fitstype)
      // println("Initial length = " + rddtyped.count().toString)

      // Recursion
      // val df_tot = recurDF(df, 1, 2)

      // Finalize
      // println(df_tot.select("1").count())
      // df_tot.show()

    }
  }
}
