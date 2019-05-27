---
permalink: /docs/api/
layout: splash
title: "Tutorial: spark-fits API"
date: 2018-06-15 22:31:13 +0200
---

# Tutorial: spark-fits API

spark-fits has API for Scala, Python, Java and R. All APIs share the
same core classes and routines, so the ways to create DataFrame from all
languages using spark-fits are identical.

### Scala

#### Linking

You can link against this library in your program at the following
coordinates in your `build.sbt`:

```scala
// %% will automatically set the Scala version needed for spark-fits
libraryDependencies += "com.github.astrolabsoftware" %% "spark-fits" % "0.8.2"

// Alternatively you can also specify directly the Scala version, e.g.
libraryDependencies += "com.github.astrolabsoftware" % "spark-fits_2.11" % "0.8.2"
```

#### Scala 2.10.6 and 2.11.X

Here is the minimal syntax in Scala 2.11 and 2.12 to play with the
package:

```scala
// Import SparkSession
import org.apache.spark.sql.SparkSession

object ReadFits extends App {
  // Initialise your SparkSession
  val spark = SparkSession
    .builder()
    .getOrCreate()

  // Read as a DataFrame a HDU of a table fits.
  val df = spark.read
    .format("fits")
    .option("hdu", <Int>)                 // [mandatory] Which HDU you want to read.
    .option("columns", <String>)          // [optional]  Comma-separated column names to load. Default loads all columns.
    .option("recordlength", <Int>)        // [optional]  If you want to define yourself the length of a record.
    .option("mode", <String>)             // [optional]  Discard empty files silently or fail fast.
    .option("verbose", <Boolean>)         // [optional]  If you want to print debugging messages on screen.
    .schema(<StructType>)                 // [optional]  If you want to bypass the header.
    .load(<String>)                       // [mandatory] Path to file or directory. Load data as DataFrame.
}
```

Note that the file can be in a local system
(`path="file://path/myfile.fits"`) or in HDFS
(`path="hdfs://<IP>:<PORT>//path/myfile.fits"`). You can also specify a
directory containing a set of FITS files
(`path="hdfs://<IP>:<PORT>//path_to_dir"`) with the same HDU structure, or you
can apply globbing patterns (`path="hdfs://<IP>:<PORT>//path_to_dir/*.fits"`), or you
can pass a string of comma-separated files (`path="hdfs://<IP>:<PORT>//path_to_dir/file1.fits,path="hdfs://<IP>:<PORT>//path_to_dir/file2.fits"`).
The connector will load the data from the same HDU from all the files in
one single DataFrame. This is particularly useful to manipulate many
small files written the same way as one.

You can specify which columns you want to load in the DataFrame, using
the option `columns`. Example,
`.option("columns", "target,Index")` will load all the data, but
will decode only these two columns. If not specified, all columns will
be loaded in the DataFrame (and you can select columns manually later).

The `recordlength` option controls how the data is split and read inside
each HDFS block (or more precisely inside each InputSplit as they are
not the same) by individual mappers for processing. By default it is set
to 1 KB. Careful for large value, you might suffer from a long garbage
collector time. The maximum size allowed for a single record to be
processed is 2\*\*31 - 1 (Int max value). But I doubt you ever need to
go as high...

The `mode` parameter controls the behaviour when reading many files. By default, it is set to `PERMISSIVE`, that is if there are empty files they will be silently discarded and the connector will not fail. Note that the empty files found will be printed on screen (WARN level of log). You can also set `mode` to `FAILFAST` to force the connector to crash if it encounters empty files. 

Note that the schema is directly inferred from the HEADER of the HDU. In
case the HEADER is not present or corrupted, you can also manually
specify it:

```scala
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
  .format("fits")
  .option("hdu", 1)
  .load(fn)

// Read as a DataFrame the first HDU of a table fits,
// and use a custom schema.
val dfCustomHeader = spark.read
  .format("fits")
  .option("hdu", 1)
  .schema(userSchema)             // bypass the header, and read the userSchema
  .load(fn)
```

### Python


See full description of options in the Scala API:

```python
## Import SparkSession
from pyspark.sql import SparkSession


if __name__ == "__main__":
  ## Initialise your SparkSession
  spark = SparkSession\
    .builder\
    .getOrCreate()

  ## Read as a DataFrame a HDU of a table fits.
  df = spark.read\
    .format("fits")\
    .option("hdu", int)\
    .option("columns", str)\
    .option("recordlength", int)\
    .option("mode", str)\
    .option("verbose", bool)\
    .schema(StructType)\
    .load(str)
```

### Java


See full description of options in the Scala API:

```java
// Import SparkSession
import org.apache.spark.sql.SparkSession

DataFrame df = spark.read()
  .format("fits")
  .option("hdu", <Int>)
  .option("columns", <String>)
  .option("recordlength", <Int>)
  .option("mode", <String>)
  .option("verbose", <Boolean>)
  .schema(<StructType>)
  .load(<String>);
```
