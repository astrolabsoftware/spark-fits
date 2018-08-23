package com.astrolabsoftware.sparkfits;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ReadFitsJ {

  static {
    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);
    spark = SparkSession.builder()
                        .master("local")
                        .appName("ReadFits")
                        .getOrCreate();
    }

  public static void main(String[] args) {
    for (int hdu : new Integer[]{2}) {
      Dataset<Row> df = spark.read()
                             .format("com.astrolabsoftware.sparkfits")
                             .option("hdu", hdu)
                             .option("verbose", true) 
                             .option("recordlength", 1 * 1024)
                             .load(args[0].toString());
      log.info("show>");
      df.show();
      log.info("printSchema>");
      df.printSchema();
      log.info("Totals rows: " + df.count());
      }
    }

  private static SparkSession spark;  
    
  private static Logger log = Logger.getLogger(ReadFitsJ.class);
  
  }