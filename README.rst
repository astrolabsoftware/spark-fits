================
Spark Fits
================

.. image:: https://travis-ci.org/JulienPeloton/spark-fits.svg?branch=master
    :target: https://travis-ci.org/JulienPeloton/spark-fits

.. contents:: **Table of Contents**

The package
================

This library provides tools to manipulate FITS data with `Apache Spark <http://spark.apache.org/>`_.
We use a "pimp my class" tactic, or in other (more polite!) words, we define
an implicit on the ``SparkSession`` to allow interactions with the FITS file format.
This is rather similar but not strictly equivalent to what was done previously for CSV.

Requirements
================

This library requires Spark 2.0+ (not tested for earlier version).

Features
================

Make a list of current features.

Quick example : Scala API
================

**Linking**

You can link against this library in your program at the following coordinates: TBD.

**Scala 2.11**

.. code:: scala

  // SparkSession
  import org.apache.spark.sql.SparkSession

  // Import the implicit to allow interaction with FITS
  import com.sparkfits.fits._

  // Initialise your SparkSession
  val spark = SparkSession
    .builder()
    .getOrCreate()

  // Read as a DataFrame the first HDU of a table fits.
  val df = spark.readfits
    .option("datatype", "table")            // we support only table for the moment
    .option("HDU", 1)                       // First HDU
    .option("printHDUHeader", false)        // to print the HEADER on the screen
    .load("src/test/resources/test.fits")   // load data as DataFrame

  // To show the schema
  df.printSchema()

  // To show the 20 top rows
  df.show()

Note that the schema is directly inferred from the HEADER of the hdu.
In case the HEADER is not present or corrupted, you can also manually specify it (TBD).

Using with Spark shell
================

This package can be added to Spark using the `--packages` command line option.
For example, to include it when starting the spark shell:

**Spark compiled with Scala 2.11**

::

  // Not yet available!
  $SPARK_HOME/bin/spark-shell --packages com.toto:spark-fits_2.11:0.Y.0

Alternatively you can build or download the jar, and add it when launching the spark shell

::

  // Available!
  $SPARK_HOME/bin/spark-shell --jars /path/to/jar/spark-fits.jar

Then just try

.. code :: scala

  scala> val df = spark.readfits
    .option("datatype", "table")
    .option("HDU", 1)
    .option("printHDUHeader", true)
    .load("src/test/resources/test.fits")
  +------ HEADER (HDU=1) ------+
  XTENSION= BINTABLE             / binary table extension
  BITPIX  =                    8 / array data type
  NAXIS   =                    2 / number of array dimensions
  NAXIS1  =                   32 / length of dimension 1
  NAXIS2  =                  100 / length of dimension 2
  PCOUNT  =                    0 / number of group parameters
  GCOUNT  =                    1 / number of groups
  TFIELDS =                    4 / number of table fields
  TTYPE1  = target
  TFORM1  = 20A
  TTYPE2  = RA
  TFORM2  = E
  TTYPE3  = Dec
  TFORM3  = E
  TTYPE4  = Redshift
  TFORM4  = E
  END
  +----------------------------+
  df: org.apache.spark.sql.DataFrame = [target: string, RA: float ... 2 more fields]

  scala> df.printSchema
  root
    |-- target: string (nullable = true)
    |-- RA: float (nullable = true)
    |-- Dec: float (nullable = true)
    |-- Redshift: float (nullable = true)

  scala> df.show(5)
  +-------+---------+----------+----------+
  | target|       RA|       Dec|  Redshift|
  +-------+---------+----------+----------+
  |NGC0000| 3.448297| 0.5586271| 1.5589794|
  |NGC0001| 4.493667|-0.7225413| 3.4817173|
  |NGC0002| 3.787274| 0.7388838| 1.8887593|
  |NGC0003| 3.423602| 1.4520081|0.89801836|
  |NGC0004|2.6619017|-0.7893153|0.12339364|
  +-------+---------+----------+----------+
  only showing top 5 rows

Building From Source
================

This library is built with SBT, and needs the `nom.tam.fits <https://github.com/nom-tam-fits/nom-tam-fits>`_ library.
To build a JAR file simply run ``sbt assembly`` from the project root.
The build configuration includes support for Scala 2.11.

Building the doc
================

Use SBT to build the doc:

::

  sbt doc
  open target/scala_2.11/api/index.html


TODO list
================

* Make the docker file
* Build against scala 2.10? Test other Spark version?
* Publish the doc.
* Add possibility for the user to provide schema. Particularly useful if the HEADER of the FITS is not there.
* ??
