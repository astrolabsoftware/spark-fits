================
Spark FITS
================

.. image:: https://travis-ci.org/JulienPeloton/spark-fits.svg?branch=master
    :target: https://travis-ci.org/JulienPeloton/spark-fits

.. image:: https://codecov.io/gh/JulienPeloton/spark-fits/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/JulienPeloton/spark-fits

.. image:: https://maven-badges.herokuapp.com/maven-central/com.github.JulienPeloton/spark-fits_2.11/badge.svg
  :target: https://maven-badges.herokuapp.com/maven-central/com.github.JulienPeloton/spark-fits_2.11

.. contents:: **Table of Contents**

The package
================

This library provides two different tools to
manipulate `FITS <https://fits.gsfc.nasa.gov/fits_home.html>`_ 
data with `Apache Spark <http://spark.apache.org/>`_:

* A Spark connector for FITS file.
* A Scala library to manipulate FITS file.

The user interface has been done to be the same as other
built-in Spark data sources (CSV, JSON, Avro, Parquet, etc).

Currently available:

* Read fits file and organize the HDU data into DataFrames.
* Automatically distribute the bintable of a FITS HDU over machines.
* Automatically infer DataFrame schema from the HDU header.

Requirements
================

This library requires Spark 2.0+ (not tested for earlier version).
The library has been tested with Scala 2.10.6 and 2.11.X.
If you want to use another version, feel free to contact us.

APIs
================

Spark FITS has API for Scala, Python, Java and R.
All APIs share the same core classes and routines, so the ways to
create DataFrame from all languages using Spark FITS are identical.

Scala
----------------

**Linking**

You can link against this library in your program at the following coordinates
in your ``build.sbt``:

.. code:: scala

  // %% will automatically set the Scala version needed for Spark FITS
  libraryDependencies += "com.github.JulienPeloton" %% "spark-fits" % "0.2.0"

  // Alternatively you can also specify directly the Scala version, e.g.
  libraryDependencies += "com.github.JulienPeloton" % "spark-fits_2.11" % "0.2.0"


**Scala 2.10.6 and 2.11.X**

Here is the minimal syntax in Scala 2.10.6 and 2.11.X
to play with the package:

.. code:: scala

  // Import SparkSession
  import org.apache.spark.sql.SparkSession

  object ReadFits extends App {
    // Initialise your SparkSession
    val spark = SparkSession
      .builder()
      .getOrCreate()

    // Read as a DataFrame a HDU of a table fits.
    val df = spark.read
      .format("com.sparkfits")
      .option("hdu", <Int>)                 // [mandatory] Which HDU you want to read.
      .option("columns", <String>)          // [optional]  Comma-separated column names to load. Default loads all columns.
      .option("recordlength", <Int>)        // [optional]  If you want to define yourself the length of a record.
      .option("verbose", <Boolean>)         // [optional]  If you want to print debugging messages on screen.
      .schema(<StructType>)                 // [optional]  If you want to bypass the header.
      .load(<String>)                       // [mandatory] Path to file or directory. Load data as DataFrame.
  }

Note that the file can be in a local system (``path="file://path/myfile.fits"``)
or in HDFS (``path="hdfs://<IP>:<PORT>//path/myfile.fits"``).
You can also specify a directory containing a set of FITS files
(``path="hdfs://<IP>:<PORT>//path_to_dir"``) with the same HDU structure.
The connector will load the data from the same HDU from all the files in one single
DataFrame. This is particularly useful to manipulate many small files written the same way as one.

You can specify which columns you want to load in the DataFrame, using the option ``columns``.
Example, ``.option("columns", List("target,Index"))`` will load all the data, but
will decode only these two columns. If not specified, all columns will be loaded in the
DataFrame (and you can select columns manually later).

The ``recordlength`` option controls how the data is split and read inside each HDFS block (or more
precisely inside each InputSplit as they are not the same) by individual mappers for processing.
By default it is set to 1 KB. Careful for large value, you might suffer from a long garbage collector time.
The maximum size allowed for a single record to be processed is 2**31 - 1 (Int max value).
But I doubt you ever need to go as high...

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
  val dfAutoHeader = spark.read
    .format("com.sparkfits")
    .option("hdu", 1)
    .load(fn)

  // Read as a DataFrame the first HDU of a table fits,
  // and use a custom schema.
  val dfCustomHeader = spark.read
    .format("com.sparkfits")
    .option("hdu", 1)
    .schema(userSchema)             // bypass the header, and read the userSchema
    .load(fn)

Python
----------------

See full description of options in the Scala API:

.. code:: python

  ## Import SparkSession
  from pyspark.sql import SparkSession


  if __name__ == "__main__":
    ## Initialise your SparkSession
    spark = SparkSession\
      .builder\
      .getOrCreate()

    ## Read as a DataFrame a HDU of a table fits.
    df = spark.read\
      .format("com.sparkfits")\
      .option("hdu", int)\
      .option("columns", str)\
      .option("recordlength", int)\
      .option("verbose", bool)\
      .schema(StructType)\
      .load(str)

Java
----------------

See full description of options in the Scala API:

.. code:: java

  // Import SparkSession
  import org.apache.spark.sql.SparkSession

  DataFrame df = spark.read()
    .format("com.sparkfits")
    .option("hdu", <Int>)
    .option("columns", <String>)
    .option("recordlength", <Int>)
    .option("verbose", <Boolean>)
    .schema(<StructType>)
    .load(<String>);

Included examples
================

Example scripts in Scala and Python, plus a Jupyter notebook
in python are included in the directory ``examples/``.

Using with spark-shell/pyspark
----------------

This package can be added to Spark using the ``--packages`` command line option.
For example, to include it when starting the spark shell (**Spark compiled with Scala 2.11**):

::

  $SPARK_HOME/bin/spark-shell --packages com.github.JulienPeloton:spark-fits_2.11:0.2.0

Using ``--packages`` ensures that this library and its dependencies will be added
to the classpath. In Python, you would do the same

::

  $SPARK_HOME/bin/pyspark --packages com.github.JulienPeloton:spark-fits_2.11:0.2.0

Alternatively to have the latest development you can download this repo and build the jar,
and add it when launching the spark shell (but won't be added in the classpath)

::

  $SPARK_HOME/bin/spark-shell --jars /path/to/jar/<spark-fits.jar>

or with pyspark

::

  $SPARK_HOME/bin/pyspark --jars /path/to/jar/<spark-fits.jar>

By default, pyspark uses a simple python shell.
It is also possible to launch PySpark in IPython, by specifying:

::

  export PYSPARK_DRIVER_PYTHON_OPTS="path/to/ipython"
  $SPARK_HOME/bin/pyspark --jars /path/to/jar/<spark-fits.jar>

Same with Jupyter notebook:

::

  cd /path/to/notebooks
  export PYSPARK_DRIVER_PYTHON_OPTS="path/to/jupyter-notebook"
  $SPARK_HOME/bin/pyspark --jars /path/to/jar/<spark-fits.jar>

See `here <https://spark.apache.org/docs/0.9.0/python-programming-guide.html>`_
for more options for pyspark.
To build the JAR, just run ``sbt ++{SBT_VERSION} package`` from the root
of the package (see ``run_*.sh`` scripts).
Here is an example in the spark-shell:

.. code :: scala

  scala> val df = spark.read
    .format("com.sparkfits")
    .option("hdu", 1)
    .option("verbose", true)
    .load("file:///path/to/spark-fits/src/test/resources/test_file.fits")
  +------ HEADER (HDU=1) ------+
  XTENSION= BINTABLE             / binary table extension
  BITPIX  =                    8 / array data type
  NAXIS   =                    2 / number of array dimensions
  NAXIS1  =                   34 / length of dimension 1
  NAXIS2  =                20000 / length of dimension 2
  PCOUNT  =                    0 / number of group parameters
  GCOUNT  =                    1 / number of groups
  TFIELDS =                    5 / number of table fields
  TTYPE1  = target
  TFORM1  = 10A
  TTYPE2  = RA
  TFORM2  = E
  TTYPE3  = Dec
  TFORM3  = D
  TTYPE4  = Index
  TFORM4  = K
  TTYPE5  = RunId
  TFORM5  = J
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

Local launch
----------------

See the two shell scripts at the root of the package

::

  ./run_scala.sh  # Scala
  ./run_python.sh # Python

Just make sure that you set up correctly the paths and the different variables.

Cluster mode
----------------

See the two shell scripts at the root of the package

::

  ./run_scala_cluster.sh  # Scala
  ./run_python_cluster.sh # Python

Just make sure that you set up correctly the paths and the different variables.

Using at NERSC
----------------

Although HPC systems are not designed for IO intensive jobs,
Spark standalone mode and filesystem-agnostic approach makes it also a
candidate to process data stored in HPC-style shared file systems such as Lustre.
Two scripts are provided at the root of the project to launch a Spark Job on Cori at NERSC:
::

  sbatch run_scala_cori.sh  # Scala
  sbatch run_python_cori.sh # Python

Keep in mind that raw performances (i.e. without any attempt to take into account
that we read from Lustre and not from HDFS for example) can be worst than in HTC systems.

Building from source
================

This library is built with SBT (see the ``build.sbt`` script provided).
To build a JAR file simply run

::

  sbt ++${SCALA_VERSION} package

from the project root. The build configuration includes support for Scala 2.10.6 and 2.11.X.
In addition you can build the doc using SBT:

::

  sbt ++{SCALA_VERSION} doc
  open target/scala_${SCALA_VERSION}/api/index.html

Running the test suite
================

To launch the test suite, just run:

::

  sbt ++${SCALA_VERSION} coverage test coverageReport

We also provide a script (test.sh) that you can run.
You should get the result on the screen, plus details of the coverage at
``target/scala_${SCALA_VERSION}/scoverage-report/index.html``.

Header Challenge!
================

The header tested so far are very simple, and not so exotic.
Over the time, we plan to add many new features based on complex examples
(see `here <https://github.com/JulienPeloton/spark-fits/tree/master/src/test/resources/toTest>`_).
If you use Spark FITS, and encounter errors while reading a header,
tell us (issues or PR) so that we fix the problem asap!

TODO list
================

* Make the docker file
* Define custom Hadoop InputFile.
* Allow image HDU manipulation.
* Test other Spark version?
* Publish the doc.


Since 23/03/18

.. image:: http://hits.dwyl.io/JulienPeloton/spark-fits.svg
    :target: http://hits.dwyl.io/JulienPeloton/spark-fits
