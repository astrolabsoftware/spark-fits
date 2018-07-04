---
permalink: /docs/installation/
layout: splash
title: "Tutorial: Installation"
date: 2018-06-15 22:31:13 +0200
redirect_from:
  - /theme-setup/
---

# Tutorial: Installation

## Requirements

This library requires Spark 2.0+ (not tested for earlier version). The
library has been tested with Scala 2.10.6 and 2.11.X. If you want to use
another version, feel free to contact us.

## Including spark-fits in your project

You can link spark-fits to your project (either `spark-shell` or `spark-submit`) by specifying the coordinates:

```bash
toto:~$ spark-submit --packages "com.github.astrolabsoftware:spark-fits_2.11:0.4.0" <...>
```

It might not contain the latest features though (see *Building from source*).
You can check the latest available version at the root of the project (see the maven central badge)

## Building from source

If you want to contribute to the project, or have access to the latest features, you can fork and clone the project, and build it from source.
This library is easily built with SBT (see the `build.sbt` script provided). To
build a JAR file simply run

```bash
toto:~$ sbt ++${SCALA_VERSION} package
```

from the project root. The build configuration includes support for
Scala 2.11. In addition you can build the doc using SBT:

```bash
toto:~$ sbt ++${SCALA_VERSION} doc
toto:~$ open target/scala_${SCALA_VERSION}/api/index.html
```

## Running the test suite

To launch the test suite, just execute:

```bash
toto:~$ sbt ++${SCALA_VERSION} coverage test coverageReport
```

We also provide a script (test.sh) that you can execute. You should get the
result on the screen, plus details of the coverage at
`target/scala_${SCALA_VERSION}/scoverage-report/index.html`.

## Using with spark-shell, pyspark or jupyter notebook

Follow the [dedicated]({{ site.baseurl }}{% link 03_interactive.md %}) tutorial!

## Batch mode and provided examples

Follow the [dedicated]({{ site.baseurl }}{% link 04_batch.md %}) tutorial!

We also include [examples](https://github.com/astrolabsoftware/spark-fits/tree/master/examples) and runners (`run_*.sh`) in the root folder of the repo.
You might have to modify those scripts with your environment.
