================
Spark Fits
================

.. contents:: **Table of Contents**

The package
================

Describe me.

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

  // Read as a DataFrame the first HDU of a table fits.
  // Also print the HEADER.
  spark.readfits
    .option("datatype", "table")    // we support only table for the moment
    .option("HDU", 1)               // First HDU
    .option("printHDUHeader", true) // just print the HEADER on the screen
    .load("/path/to/myfits")        // load data as DataFrame

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


Building From Source
================

This library is built with SBT, and needs the `nom.tam.fits <https://github.com/nom-tam-fits/nom-tam-fits>`_ library.
To build a JAR file simply run ``sbt assembly`` from the project root.
The build configuration includes support for Scala 2.11.

TODO list
================

Make a list of wishes!
