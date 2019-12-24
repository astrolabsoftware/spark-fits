/*
 * Copyright 2018 AstroLab Software
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
                        .appName("ReadFitsJ")
                        .getOrCreate();
    }

  public static void main(String[] args) {
    for (int hdu : new Integer[]{2}) {
      Dataset<Row> df = spark.read()
                             .format("fits")
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
