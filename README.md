# FITS Data Source for Apache Spark

[![Build Status](https://travis-ci.org/astrolabsoftware/spark-fits.svg?branch=master)](https://travis-ci.org/astrolabsoftware/spark-fits)
[![codecov](https://codecov.io/gh/astrolabsoftware/spark-fits/branch/master/graph/badge.svg?style=platic)](https://codecov.io/gh/astrolabsoftware/spark-fits)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.astrolabsoftware/spark-fits_2.11/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.astrolabsoftware/spark-fits_2.11)
[![Arxiv](http://img.shields.io/badge/arXiv-1804.07501-yellow.svg?style=platic)](https://arxiv.org/abs/1804.07501)

## Latest news

- [01/2018] **Launch**: project starts!
- [03/2018] **Release**: version 0.3.0
- [04/2018] **Paper**: [![Arxiv](http://img.shields.io/badge/arXiv-1804.07501-yellow.svg?style=platic)](https://arxiv.org/abs/1804.07501)
- [05/2018] **Release**: version 0.4.0
- [06/2018] **New location**: spark-fits is an official project of [AstroLab](https://astrolabsoftware.github.io/)!
- [07/2018] **Release**: version 0.5.0, 0.6.0
- [10/2018] **Release**: version 0.7.0, 0.7.1
- [12/2018] **Release**: version 0.7.2
- [03/2019] **Release**: version 0.7.3
- [05/2019] **Release**: version 0.8.0, 0.8.1, 0.8.2
- [06/2019] **Release**: version 0.8.3
- [05/2020] **Release**: version 0.8.4

## spark-fits

This library provides two different tools to manipulate
[FITS](https://fits.gsfc.nasa.gov/fits_home.html) data with [Apache
Spark](http://spark.apache.org/):

-   A Spark connector for FITS file.
-   A Scala library to manipulate FITS file.

The user interface has been done to be the same as other built-in Spark
data sources (CSV, JSON, Avro, Parquet, etc). Note that spark-fits follows Apache Spark Data Source V1 ([plan](https://github.com/astrolabsoftware/spark-fits/issues/50) to migrate to V2). See our [website](https://astrolabsoftware.github.io/spark-fits/) for more information. To include spark-fits in your job:

```bash
# Scala 2.11
spark-submit --packages "com.github.astrolabsoftware:spark-fits_2.11:0.8.4" <...>

# Scala 2.12
spark-submit --packages "com.github.astrolabsoftware:spark-fits_2.12:0.8.4" <...>
```

or you can link against this library in your program at the following coordinates in your build.sbt

```scala
// Scala 2.11
libraryDependencies += "com.github.astrolabsoftware" % "spark-fits_2.11" % "0.8.4"

// Scala 2.12
libraryDependencies += "com.github.astrolabsoftware" % "spark-fits_2.12" % "0.8.4"
```

Currently available:

-   Read fits file and organize the HDU data into DataFrames.
-   Automatically distribute bintable rows over machines.
-   Automatically distribute image rows over machines.
-   Automatically infer DataFrame schema from the HDU header.

## Header Challenge!

The header tested so far are very simple, and not so exotic. Over the
time, we plan to add many new features based on complex examples (see
[here](https://github.com/astrolabsoftware/spark-fits/tree/master/src/test/resources/toTest)).
If you use spark-fits, and encounter errors while reading a header, tell
us (issues or PR) so that we fix the problem asap!

## TODO list

- Define custom Hadoop InputFile.
- Migrate to Spark DataSource V2

## Support

<p align="center"><img width="100" src="https://github.com/astrolabsoftware/spark-fits/raw/master/pic/lal_logo.jpg"/> <img width="100" src="https://github.com/astrolabsoftware/spark-fits/raw/master/pic/psud.png"/> <img width="100" src="https://github.com/astrolabsoftware/spark-fits/raw/master/pic/1012px-Centre_national_de_la_recherche_scientifique.svg.png"/></p>
