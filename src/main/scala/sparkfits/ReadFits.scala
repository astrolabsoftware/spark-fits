package sparkfits

import org.apache.spark.sql.SparkSession

import org.apache.log4j.Level
import org.apache.log4j.Logger

import sparkfits.fits._

object ReadFits {
  val spark = SparkSession
    .builder()
    .getOrCreate()

  def main(args : Array[String]) = {

    // val df = spark.read.fits(args(0).toString)
    val df = spark.readfits
      .option("printme", "toto")
      .option("HDU", 1)
      .load(args(0).toString)

    df.show()
    df.printSchema()
  }

}

// object ReadFits {
//
//   // Set to Level.WARN is you want verbosity
//   // Logger.getLogger("org").setLevel(Level.OFF)
//   // Logger.getLogger("akka").setLevel(Level.OFF)
//
//   // Initialise Spark context
//   val conf = new SparkConf()
//   val sc = new SparkContext(conf)
//   val sq = new SQLContext(sc)
  // val spark = SparkSession
  //   .builder()
  //   .getOrCreate()
//
//   // Internal classes
//   val sfu = new SparkFitsUtil
//
  // // For implicit conversions like converting RDDs to DataFrames
  // import spark.implicits._
//
  // /**
  //   * Returns a block of rows as a Vector of Tuples.
  //   * Useful to turn RDD into DF.
  //   * /!\ return tuple of Any (you need a conversion later on).
  //   *
  //   * @param x : nom.tam.fits.Fits
  //   *             Instance of Fits.
  //   * @param indexHDU : int
  //   *             The HDU to be read.
  //   * @param offset : int
  //   *             Initial position of the cursor (first row)
  //   * @param sizeBlock : int
  //   *             Number of row to read.
  //   * @param nRowMax : int
  //   *             Number total of rows in the HDU.
  //   */
  // def yieldRows(x : Fits,
  //     indexHDU : Int,
  //     offset : Int,
  //     sizeBlock : Int,
  //     nRowMax : Int) = {
  //
  //   // Start of the block
  //   val start = offset * sizeBlock
  //
  //   // End of the block
  //   val stop_tmp = (offset + 1) * sizeBlock - 1
  //   val stop = if (stop_tmp < nRowMax - 1) {
  //     stop_tmp
  //   } else {
  //     nRowMax - 1
  //   }
  //
  //   // Yield rows
  //   for {
  //     i <- start to stop
  //   } yield (
  //     // Get the data as Array[Table]
  //     Array(x.getHDU(indexHDU).asInstanceOf[BinaryTableHDU]
  //     // Get ith row as an Array[Object]
  //     .getRow(i)
  //     // Get each element inside Objects
  //     // /!\ Strings are not returned as Object...
  //     .map {
  //       case x : Array[_] => x.asInstanceOf[Array[_]](0)
  //       case x : String => x
  //     }
  //   // Map to tuple to allow the conversion to DF later on
  //   ).map {
  //     case x => (x(0).toString, x(1).toString, x(2).toString)
  //   }.toList(0)) // Need a better handling of that...
  // }
//
//   def yieldRows2(x : Fits,
//       indexHDU : Int,
//       offset : Int,
//       sizeBlock : Int,
//       nRowMax : Int) = {
//
//     // Start of the block
//     val start = offset * sizeBlock
//
//     // End of the block
//     val stop_tmp = (offset + 1) * sizeBlock - 1
//     val stop = if (stop_tmp < nRowMax - 1) {
//       stop_tmp
//     } else {
//       nRowMax - 1
//     }
//
//     val nColMax = 4
//
//     // Yield rows
//     for {
//       i <- start to stop
//     } yield (
//       // Get the data as Array[Table]
//       Array(x.getHDU(indexHDU).asInstanceOf[BinaryTableHDU]
//       // Get ith row as an Array[Object]
//       .getRow(i)
//       // Get each element inside Objects
//       // /!\ Strings are not returned as Object...
//       .map {
//         case x : Array[_] => x.asInstanceOf[Array[_]](0)
//         case x : String => x
//       }
//     // Map to tuple to allow the conversion to DF later on
//     ).map { x => Row.fromSeq(x)}.toList(0)) // Need a better handling of that...
//   }
//
//   /**
//     * Returns a block of row indices as a Vector of Tuples.
//     *
//     * @param offset : int
//     *             Initial position of the cursor (first row)
//     * @param sizeBlock : int
//     *             Number of row to read.
//     * @param nRowMax : int
//     *             Number total of rows in the HDU.
//     */
//   def yieldIdRows(offset : Int, sizeBlock : Int, nRowMax : Int) = {
//
//     // Start of the block
//     val start = offset * sizeBlock
//
//     // End of the block
//     val stop_tmp = (offset + 1) * sizeBlock - 1
//     val stop = if (stop_tmp < nRowMax - 1) {
//       stop_tmp
//     } else {
//       nRowMax - 1
//     }
//
//     // Yield indices
//     for {
//       i <- start to stop
//     } yield (i)
//   }
//
//   def main(args : Array[String]) {
//     // Open the fits
//     val ffits = new Fits(args(0).toString) with Serializable
//
//     // Get the total number of HDUs
//     val nHDUs = sfu.getNHdus(ffits, 0)
//
//     println("Number of HDUs : " + nHDUs.toString)
//
//     // Process only HDU > 0
//     for (indexHDU <- 1 to nHDUs - 1) {
//       println(s"Processing HDU number $indexHDU")
//
//       // Get the hdu
//       val hdu = ffits.getHDU(indexHDU)
//
//       // Check it is a table
//       val isTable = hdu.isInstanceOf[BinaryTableHDU]
//       if (isTable == false) {
//         System.err.println("Work only with BinaryTableHDU!")
//         System.exit(1)
//       }
//
//       val data = hdu.asInstanceOf[BinaryTableHDU]
//
//       val ncols = data.getNCols
//       val nrows = data.getNRows
//       println(s"Table with [col x row] = [$ncols x $nrows]")
//
//       val nBlock = 100
//       val sizeBlock : Int = nrows / nBlock
//       val nParts = 100
//       val col0 = 0
//
//       val it = data.getHeader.iterator
//       val myheader = sfu.getMyHeader(it, "")
//
//       myheader.split(",").foreach(println)
//
//       println(data.getColumnName(0))
//       println(data.getColumnName(1))
//       println(data.getColumnFormat(0))
//       println(data.getColumnFormat(1))
//       val fitsSchema = StructType(
//         List(
//           StructField("ra", DoubleType, false),
//           StructField("dec", DoubleType, false)
//         )
//       )
//
//       // Initialisation (1st column)
//       val rdd = sc.parallelize(0 to nBlock - 1, nParts)
//         .map(blockid => (blockid, new Fits(args(0).toString) with Serializable ))
//         .map(x => yieldRows(x._2, indexHDU, x._1, sizeBlock, nrows))
//         .flatMap(x => x)
//
//       val df = rdd.toDF()
//       df.show()
//       df.printSchema()
//       // Get the correct type for the column
//       // Type of the data
//       // val fitstype : String = data.getColumnFormat(col0)
//       // val df = getDF(rdd, fitstype, col0.toString)
//       // val rddtyped = getRDDType(rdd, fitstype)
//       // println("Initial length = " + rddtyped.count().toString)
//
//       // Recursion
//       // val df_tot = recurDF(df, 1, 2)
//
//       // Finalize
//       // println(df_tot.select("1").count())
//       // df_tot.show()
//
//     }
//   }
// }
