---
layout: posts
title: About
permalink: /about/
author_profile: true
---

## spark-fits

This library provides two different tools to manipulate
[FITS](https://fits.gsfc.nasa.gov/fits_home.html) data with [Apache
Spark](http://spark.apache.org/):

-   A Spark connector for FITS file.
-   A Scala library to manipulate FITS file.

The user interface has been done to be the same as other built-in Spark
data sources (CSV, JSON, Avro, Parquet, etc).

Currently available:

-   Read fits file and organize the HDU data into DataFrames.
-   Automatically distribute bintable rows over machines.
-   Automatically distribute image rows over machines.
-   Automatically infer DataFrame schema from the HDU header.

## Support

![LAL]({{ "/assets/images/lal_logo.jpg" | absolute_url }})
![UPSUD]({{ "/assets/images/Logo_Université_Paris-Saclay.svg.png" | absolute_url }})
![CNRS]({{ "/assets/images/1012px-Centre_national_de_la_recherche_scientifique.svg.png" | absolute_url }})
