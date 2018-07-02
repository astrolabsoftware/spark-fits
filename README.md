# FITS Data Source for Apache Spark

[![Build Status](https://travis-ci.org/theastrolab/spark-fits.svg?branch=master)](https://travis-ci.org/theastrolab/spark-fits)
[![codecov](https://codecov.io/gh/theastrolab/spark-fits/branch/master/graph/badge.svg?style=platic)](https://codecov.io/gh/theastrolab/spark-fits)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.theastrolab/spark-fits_2.11/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.theastrolab/spark-fits_2.11)
[![Arxiv](http://img.shields.io/badge/arXiv-1804.07501-yellow.svg?style=platic)](https://arxiv.org/abs/1804.07501)

## Latest news

- [01/2018] **Launch**: spark-fits is created!
- [03/2018] **Release**: version 0.3.0
- [04/2018] **Paper**: [![Arxiv](http://img.shields.io/badge/arXiv-1804.07501-yellow.svg?style=platic)](https://arxiv.org/abs/1804.07501)
- [04/2018] **Talk**: Statistical challenges for large-scale structure in the era of LSST, Oxford. [slides](intensitymapping.physics.ox.ac.uk/SCLSS/Slides/Peloton.pdf)
- [05/2018] **Release**: version 0.4.0
- [06/2018] **New location**: spark-fits is an official project of [AstroLab](https://theastrolab.github.io/)!

## spark-fits

This library provides two different tools to manipulate
[FITS](https://fits.gsfc.nasa.gov/fits_home.html) data with [Apache
Spark](http://spark.apache.org/):

-   A Spark connector for FITS file.
-   A Scala library to manipulate FITS file.

The user interface has been done to be the same as other built-in Spark
data sources (CSV, JSON, Avro, Parquet, etc). See our [website](https://theastrolab.github.io/spark-fits/) for more information.

Currently available:

-   Read fits file and organize the HDU data into DataFrames.
-   Automatically distribute bintable rows over machines.
-   Automatically distribute image rows over machines. **new in 0.4.0**
-   Automatically infer DataFrame schema from the HDU header.

## Header Challenge!

The header tested so far are very simple, and not so exotic. Over the
time, we plan to add many new features based on complex examples (see
[here](https://github.com/theastrolab/spark-fits/tree/master/src/test/resources/toTest)).
If you use spark-fits, and encounter errors while reading a header, tell
us (issues or PR) so that we fix the problem asap!

## TODO list

- Define custom Hadoop InputFile.
- Adapt to S3 usage?

## Support

<p align="center"><img width="100" src="https://github.com/theastrolab/spark-fits/raw/master/pic/lal_logo.jpg"/> <img width="100" src="https://github.com/theastrolab/spark-fits/raw/master/pic/psud.png"/> <img width="100" src="https://github.com/theastrolab/spark-fits/raw/master/pic/1012px-Centre_national_de_la_recherche_scientifique.svg.png"/></p>

| Since 23/04/18 | ![image](http://hits.dwyl.io/theastrolab/spark-fits.svg%0A%20:target:%20http://hits.dwyl.io/theastrolab/spark-fits) |
|:-----:|:-----:
