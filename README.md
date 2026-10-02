# FITS Data Source for Apache Spark

[![CI](https://github.com/astrolabsoftware/spark-fits/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/astrolabsoftware/spark-fits/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.astrolabsoftware/spark-fits_2.12)](https://central.sonatype.com/artifact/com.github.astrolabsoftware/spark-fits_2.12)
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
- [07/2020] **Release**: version 0.9.0
- [04/2021] **Release**: version 1.0.0

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
spark-submit --packages "com.github.astrolabsoftware:spark-fits_2.11:1.0.0" <...>

# Scala 2.12
spark-submit --packages "com.github.astrolabsoftware:spark-fits_2.12:1.0.0" <...>
```

or you can link against this library in your program at the following coordinates in your build.sbt

```scala
// Scala 2.11
libraryDependencies += "com.github.astrolabsoftware" % "spark-fits_2.11" % "1.0.0"

// Scala 2.12
libraryDependencies += "com.github.astrolabsoftware" % "spark-fits_2.12" % "1.0.0"
```

For the next release, the build targets Scala 2.12 (Spark 3.4.4) and Scala
2.13 (Spark 4.2.0). CI also builds and tests with Spark 2.4.8 and 3.5.9
on Scala 2.12. Match the Scala binary version of your Spark installation; use
the latest published version of the corresponding `spark-fits_2.12` or
`spark-fits_2.13` artifact. Spark is a `provided` dependency and is not
bundled into spark-fits.

To run the build locally, use `sbt test` (Scala 2.12 / Spark 3.4.4).
Set `SPARK_VERSION` and use `++2.12.21` or `++2.13.18` to select another CI
combination. Release instructions are in [docs/releasing.md](docs/releasing.md).

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
