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

  class BinaryTableHDUExt(x : Header, y : BinaryTable) extends BinaryTableHDU(x, y) with java.io.Serializable

  // case class FitsData(key1: String, val: Long)

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

      // // Actually it cannot work with image
      // val hduExt = hdu match {
      //   case image : ImageHDU => image
      //   case table : BinaryTableHDU => table
      // }

      val data = hdu.asInstanceOf[BinaryTableHDU]

      val ncols = data.getNCols
      val nrows = data.getNRows
      println(s"Table with [col x row] = [$ncols x $nrows]")


      // def returnLine()
      // val mR = data.getData().getModelRow()
      // var n : Int = 0
      // if (data.getData.reset()) {
      //   val arrayDataInput = ffits.getStream()
      //   arrayDataInput.skip(24) // skip one line
      //   while (n < 10) {
      //     n = n + 1
      //     // val arr = Seq(arrayDataInput.readDouble, arrayDataInput.readDouble, arrayDataInput.readDouble)
      //     def arr(arrayDataInput : ArrayDataInput) : Seq[Double] = Seq(arrayDataInput.readDouble, arrayDataInput.readDouble, arrayDataInput.readDouble)
      //     val rdd = sc.parallelize(0 to 10).map(x => arr(arrayDataInput))
      //     val df = rdd.toDF("value")
      //     df.show()
      //   }
      // }
      // println("Count : " + n.toString)

      // def readStream(rdd : RDD[Double] = sc.parallelize(Seq[Double]()), n : Int = 0, nmax : Int = 2) : RDD[Double] = {
      //   if (n == nmax) {
      //     rdd
      //   } else {
      //     val itd = data.getColumn(n).asInstanceOf[Array[Double]]
      //     val newRdd = sc.parallelize(itd) ++ rdd
      //     buildMyRDD(newRdd, n + 1)
      //   }
      // }
      // val rdd = readStream()

      val it = data.getHeader.iterator
      val myheader = getMyHeader(it, "")

      // val toto = header(data.getHeader)
      // println(toto)
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

      // val dec = (0 to nrows - 1)
      //   .map(
      //     x => data.getElement(x, 1)
      //       .asInstanceOf[Array[Double]]
      //       .seq).toSeq

      // val rdd = sc.parallelize(Seq((1, "Spark"), (2, "Databricks"), (3, "Notebook")))
      // val df = rdd.toDF("Id", "Name")

      // Solution degueu
      // val dataset = (0 to nrows - 1)
      //   .flatMap(x => Seq( (proc.get(data, x, 0), proc.get(data, x, 1)) ))
      // val rdd = sc.parallelize(dataset)

      // def myIt(it : Iterator[Double], rdd : RDD[Seq[Double]] = sc.parallelize(Seq[Seq[Double]]())) : RDD[Seq[Double]] = {
      //   if (!it.hasNext) {
      //     rdd
      //   } else {
      //     // rdd.cache()
      //     // val d = it.next()
      //     // val newRdd = sc.parallelize(Seq(d)) ++ rdd
      //     val newRdd = sc.parallelize(0 to 1000)
      //       .map(x => Seq(it.next())) ++ rdd
      //     // rdd.unpersist()
      //     myIt(it, newRdd)
      //   }
      // }
      // val itd = data.getColumn(0).asInstanceOf[Array[Double]].toList.iterator
      // val rdd = myIt(itd)

      // def getRowsIt(
      //     data : BinaryTableHDU,
      //     blocksize : Int,
      //     rdd : RDD[Seq[Double]] = sc.parallelize(Seq[Seq[Double]]()),
      //     index : Int = 0,
      //     maxIndex : Int = 100) : RDD[Seq[Double]] = {
      //   if (index == maxIndex) {
      //     rdd
      //   } else {
      //     // rdd.cache()
      //     // val d = it.next()
      //     // val newRdd = sc.parallelize(Seq(d)) ++ rdd
      //     val stop = index + blocksize
      //     val newRdd = sc.parallelize(index to stop)
      //       .map(x => proc.get(data, x)) ++ rdd
      //       // .map(x => data.getRow(x).seq.asInstanceOf[Seq[Array[Double]]].flatMap(x => x)) ++ rdd
      //     // rdd.unpersist()
      //     getRowsIt(data, blocksize, newRdd, index + blocksize, maxIndex)
      //   }
      // }
      // val rdd = getRowsIt(data, 10)

      // def buildMyRDD(rdd : RDD[Double] = sc.parallelize(Seq[Double]()), n : Int = 0, nmax : Int = 2) : RDD[Double] = {
      //   if (n == nmax) {
      //     rdd
      //   } else {
      //     val itd = data.getColumn(n).asInstanceOf[Array[Double]]
      //     val newRdd = sc.parallelize(itd) ++ rdd
      //     buildMyRDD(newRdd, n + 1)
      //   }
      // }
      // val rdd = buildMyRDD()

      // def buildMyRDD(rdd : RDD[Double] = sc.parallelize(Seq[Double]()), n : Int = 0, nmax : Int = 100) : RDD[Double, Double, Double] = {
      //   if (n == nmax) {
      //     rdd
      //   } else {
      //     val itd = data.getRow(n).asInstanceOf[Array[Double]]
      //     val newRdd = sc.parallelize(Seq(itd)) ++ rdd
      //     buildMyRDD(newRdd, n + 1)
      //   }
      // }
      // val rdd = buildMyRDD()


      // val get = (f : Fits, col : Int, row : Int) => f.getHDU(1).asInstanceOf[BinaryTableHDU].getRow(0).seq.asInstanceOf[Seq[Array[Double]]].flatMap(x=>x)
      // Solution plus propre - mais qui ne marche pas... :-(

      // Initialisation
      val rdd = sc.binaryFiles(args(0).toString, 20000)
        .map(x => new Fits(x._1))
        .map(x => x.getHDU(1))
        .map(x => x.asInstanceOf[BinaryTableHDU])
        .map(x => x.getColumn(0).asInstanceOf[Array[Double]].zipWithIndex)
        .flatMap(x => x)
      // val rdd = sc.binaryFiles(args(0).toString, 20000)
      //   .map(x => new Fits(x._1))
      //   .map(x => x.getHDU(1))
      //   .map(x => x.asInstanceOf[BinaryTableHDU])
      //   .map(x => x.getElement(0, 0).seq.asInstanceOf[Seq[Array[Double]]].flatMap(x=>x).zipWithIndex)
      //   .flatMap(x => x)

      // val df = rdd.toDF("RA", "Dec")
      val df = rdd.toDF("RA", "id")
      // df.show()

      def recurDF(df : DataFrame, fn : String, n : Int, nmax : Int) : DataFrame = {
        if (n == nmax + 1) {
          df
        } else {
          val df_tmp = sc.binaryFiles(fn, 20000)
            .map(x => new Fits(x._1))
            .map(x => x.getHDU(1))
            .map(x => x.asInstanceOf[BinaryTableHDU])
            .map(x => x.getColumn(n).asInstanceOf[Array[Double]].zipWithIndex)
            .flatMap(x => x).toDF(n.toString, "id")
          recurDF(df.join(df_tmp, "id"), fn, n + 1, nmax)
        }
      }

      def recurDfRow(df : DataFrame, fn : String, n : Int, nmax : Int) : DataFrame = {
        if (n == nmax + 1) {
          df
        } else {
          val df_tmp = sc.binaryFiles(fn, 20000)
            .map(x => new Fits(x._1))
            .map(x => x.getHDU(1))
            .map(x => x.asInstanceOf[BinaryTableHDU])
            .map(x => x.getRow(n).seq.asInstanceOf[Seq[Array[Double]]].flatMap(x=>x).zipWithIndex)
            .flatMap(x => x).toDF(n.toString, "id")
          recurDfRow(df.join(df_tmp, "id"), fn, n + 1, nmax)
        }
      }

      val df2 = recurDF(df, args(0).toString, 1, 1)
      // val df2 = recurDfRow(df, args(0).toString, 1, 2)
      // df2.drop("id").show()
      // df2.select()
      df2.select("id").filter(x => x(0) == 100).show()

      // sc.parallelize(1 to 1000000, 1000).map(x => (x, new Fits(fn) with Serializable)).map(x=>(x._1, x._2.getHDU(1).asInstanceOf[BinaryTableHDU].getElement(x._1, 1).asInstanceOf[Array[Float]](0))).toDF.count()

      // println(dec.take(10))
      // val rows = dec.map{x => Row(x:_*)}
      // val rdd = sc.makeRDD(rows)
      // val df = sq.createDataFrame(rdd, fitsSchema)
      // val df = dec.toDS()

      // val df_filtered = df.filter($"dec" < 0.0)
      // df_filtered.show()
      // println(df_filtered.count())

    }
  }
}
