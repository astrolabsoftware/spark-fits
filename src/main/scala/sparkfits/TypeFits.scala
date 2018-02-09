// package sparkfits
//
// import java.math.BigDecimal
// import java.sql.{Date, Timestamp}
// import java.text.{SimpleDateFormat, NumberFormat}
// import java.util.Locale
//
// import org.apache.spark.sql.types._
//
// import scala.util.Try
//
// object TypeFits {
//   def castTo(datum : String, fitstype: String) : Any = {
//     fitstype match {
//       case "D" => datum.toDouble
//       case "1E" => datum.toDouble
//       case "1J" => datum.toInt
//       case _ => datum
//       // case _: FloatType => Try(datum.toFloat)
//       //   .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).floatValue())
//       // case _: DoubleType => Try(datum.toDouble)
//       //   .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).doubleValue())
//       // case _: BooleanType => datum.toBoolean
//       // case _: DecimalType => new BigDecimal(datum.replaceAll(",", ""))
//       // case _: TimestampType if dateFormatter != null =>
//       //   new Timestamp(dateFormatter.parse(datum).getTime)
//       // case _: TimestampType => Timestamp.valueOf(datum)
//       // case _: DateType if dateFormatter != null =>
//       //   new Date(dateFormatter.parse(datum).getTime)
//       // case _: DateType => Date.valueOf(datum)
//       // case _: StringType => datum
//       // case _ => throw new RuntimeException(s"Unsupported type: ${castType.typeName}")
//     }
//   }
// }
