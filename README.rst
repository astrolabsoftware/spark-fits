================
Spark Fits
================

.. image:: https://travis-ci.org/JulienPeloton/spark-fits.svg?branch=master
    :target: https://travis-ci.org/JulienPeloton/spark-fits

.. contents:: **Table of Contents**

The package
================

This library provides two different tools to manipulate FITS data with `Apache Spark <http://spark.apache.org/>`_:

* A Spark connector for FITS file.
* A Scala library to manipulate FITS file.

From the user point of view, we use a "pimp my class" tactic, or in other words, we define
an implicit on the ``SparkSession`` to allow interactions with the FITS file format.
This is rather similar but not strictly equivalent to what was done previously for CSV.
In addition we developed the necessary tools to interpret the FITS file format
in HDFS by extending FileInputFormat and RecordReader Hadoop classes.

Requirements
================

This library requires Spark 2.0+ (not tested for earlier version).
The library has been tested with Scala 2.10.6 and 2.11.X. If you want to use another
version, feel free to contact us.

Features
================

* Read fits table and organize the HDU data into DataFrames.
* Automatically distribute the data of a FITS HDU over machines.
* Automatically infer DataFrame schema from the HDU header. Alternatively, users can specify the schema.

Provided Examples
================

We provide two shell scripts to show the use of the library:

**Local use**

::

  ./run.sh

**Spark standalone**

::

  ./run_cluster.sh

Just make sure that you set up correctly the paths and the different variables.

Scala API
================

**Linking**

You can link against this library in your program at the following coordinates: TBD.

**Scala 2.10.6 and 2.11.X**

.. code:: scala

  // Import SparkSession
  import org.apache.spark.sql.SparkSession

  // Import the implicit to allow interaction with FITS
  import com.sparkfits.fits._

  object ReadFits extends App {
    // Initialise your SparkSession
    val spark = SparkSession
      .builder()
      .getOrCreate()

    // Read as a DataFrame a HDU of a table fits.
    val df = spark.readfits
      .option("datatype", "table")               // [mandatory] We support only table for the moment.
      .option("HDU", <Int>)                      // [mandatory] Which HDU you want to read.
      .option("recordLength", <Long>)            // [optional]  If you want to define yourself the length of a record.
      .option("printHDUHeader", <Boolean>)       // [optional]  If you want to print the HEADER on the screen.
      .schema(<StructType>)                      // [optional]  If you want to bypass the header.
      .load("src/test/resources/test_file.fits") // [mandatory] Load data as DataFrame.
  }

The `recordLength` option controls how the data is split and read inside each HDFS block (or more
precisely inside each InputSplit as those are not the same) by individual mappers for processing.
By default it is set to 128 KB. Careful for large value, you might suffer from a long garbage collector time.

Note that the schema is directly inferred from the HEADER of the HDU.
In case the HEADER is not present or corrupted, you can also manually specify it:

.. code:: scala

  // Specify manually the columns for the first HDU with their data types.
  // Note that you need to know in advance what is in the HDU (number
  // of columns and data types).
  val userSchema = StructType(
    List(
      StructField("toto", StringType, true),
      StructField("tutu", FloatType, true),
      StructField("tata", DoubleType, true),
      StructField("titi", LongType, true),
      StructField("tete", IntegerType, true)
    )
  )

  // Read as a DataFrame the first HDU of a table fits,
  // and infer schema from the header.
  val dfAutoHeader = spark.readfits
    .option("datatype", "table")
    .option("HDU", 1)
    .load(fn)

  // Read as a DataFrame the first HDU of a table fits,
  // and use a custom schema.
  val dfCustomHeader = spark.readfits
    .option("datatype", "table")
    .option("HDU", 1)
    .schema(userSchema)             // bypass the header, and read the userSchema
    .load(fn)

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

To build the JAR, just run `sbt ++{SBT_VERSION} package` from the root
of the package (see run_*.sh scripts). Then in the spark-shell

.. code :: scala

  scala> import com.sparkfits.fits._
  scala> val df = spark.readfits
    .option("datatype", "table")
    .option("HDU", 1)
    .option("recordLength", 128 * 1024) // 128 KB per record
    .option("printHDUHeader", true)
    .load("src/test/resources/test_file.fits")
  +------ HEADER (HDU=1) ------+
  XTENSION= 'BINTABLE'           / binary table extension
  BITPIX  =                    8 / array data type
  NAXIS   =                    2 / number of array dimensions
  NAXIS1  =                   34 / length of dimension 1
  NAXIS2  =                20000 / length of dimension 2
  PCOUNT  =                    0 / number of group parameters
  GCOUNT  =                    1 / number of groups
  TFIELDS =                    5 / number of table fields
  TTYPE1  = 'target  '
  TFORM1  = '10A     '
  TTYPE2  = 'RA      '
  TFORM2  = 'E       '
  TTYPE3  = 'Dec     '
  TFORM3  = 'D       '
  TTYPE4  = 'Index   '
  TFORM4  = 'K       '
  TTYPE5  = 'RunId   '
  TFORM5  = 'J       '
  END
  +----------------------------+
  df: org.apache.spark.sql.DataFrame = [target: string, RA: float ... 3 more fields]

  scala> df.printSchema
  root
   |-- target: string (nullable = true)
   |-- RA: float (nullable = true)
   |-- Dec: double (nullable = true)
   |-- Index: long (nullable = true)
   |-- RunId: integer (nullable = true)

  scala> df.show(5)
  +----------+---------+--------------------+-----+-----+
  |    target|       RA|                 Dec|Index|RunId|
  +----------+---------+--------------------+-----+-----+
  |NGC0000000| 3.448297| -0.3387486324784641|    0|    1|
  |NGC0000001| 4.493667| -1.4414990980543227|    1|    1|
  |NGC0000002| 3.787274|  1.3298379564211742|    2|    1|
  |NGC0000003| 3.423602|-0.29457151504987844|    3|    1|
  |NGC0000004|2.6619017|  1.3957536426732444|    4|    1|
  +----------+---------+--------------------+-----+-----+
  only showing top 5 rows

Building From Source
================

This library is built with SBT (see the build.sbt script provided).
To build a JAR file simply run

::

  sbt ++${SCALA_VERSION} package

from the project root. The build configuration includes support for Scala 2.10.6 and 2.11.X.

Running the test suite
================

To launch the test suite, just run:

::

  sbt ++${SCALA_VERSION} coverage test coverageReport

We also provide a script (test.sh) that you can run.
You should get the result on the screen, plus details of the coverage at
``target/scala_${SCALA_VERSION}/scoverage-report/index.html``.

Building the doc
================

Use SBT to build the doc:

::

  sbt ++{SCALA_VERSION} doc
  open target/scala_${SCALA_VERSION}/api/index.html


TODO list
================

* Make the docker file
* Define custom Hadoop InputFile.
* Test other Spark version?
* Publish the doc.
