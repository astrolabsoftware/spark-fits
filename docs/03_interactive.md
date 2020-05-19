---
permalink: /docs/interactive/
layout: splash
title: "Tutorial: Using spark-fits with spark-shell, pyspark or jupyter notebook"
date: 2018-06-15 22:31:13 +0200
---

# Tutorial: Using spark-fits with spark-shell, pyspark or jupyter notebook

## Using with spark-shell/pyspark

This package can be added to Spark using the `--packages` command line
option. For example, to include it when starting the spark shell:

```bash
# Scala 2.11
$SPARK_HOME/bin/spark-shell --packages com.github.astrolabsoftware:spark-fits_2.11:0.8.4

# Scala 2.12
$SPARK_HOME/bin/spark-shell --packages com.github.astrolabsoftware:spark-fits_2.12:0.8.4
```

Using `--packages` ensures that this library and its dependencies will
be added to the classpath (make sure you use the latest version). In Python, you would do the same

```bash
# Scala 2.11
$SPARK_HOME/bin/pyspark --packages com.github.astrolabsoftware:spark-fits_2.11:0.8.4

# Scala 2.12
$SPARK_HOME/bin/pyspark --packages com.github.astrolabsoftware:spark-fits_2.12:0.8.4
```

Alternatively to have the latest development you can download this repo
and build the jar, and add it when launching the spark shell (but won't
be added in the classpath)

```bash
$SPARK_HOME/bin/spark-shell --jars /path/to/jar/<spark-fits.jar>
```

or with pyspark

```bash
$SPARK_HOME/bin/pyspark --jars /path/to/jar/<spark-fits.jar>
```
By default, pyspark uses a simple python shell. It is also possible to
launch PySpark in IPython, by specifying:

```bash
export PYSPARK_DRIVER_PYTHON_OPTS="path/to/ipython"
$SPARK_HOME/bin/pyspark --jars /path/to/jar/<spark-fits.jar>
```
Same with Jupyter notebook:

```bash
cd /path/to/notebooks
export PYSPARK_DRIVER_PYTHON_OPTS="path/to/jupyter-notebook"
$SPARK_HOME/bin/pyspark --jars /path/to/jar/<spark-fits.jar>
```
See
[here](https://spark.apache.org/docs/0.9.0/python-programming-guide.html)
for more options for pyspark. To build the JAR, just run
`sbt ++{SBT_VERSION} package` from the root of the package (see
`run_*.sh` scripts). Here is an example in the spark-shell:

## Using with Jupyter Notebook

We provide notebooks (pyspark) in the section [example](https://github.com/astrolabsoftware/spark-fits/tree/master/examples/jupyter).
For notebook in Scala/Spark (using the Toree kernel), see the [spark3d](https://github.com/astrolabsoftware/spark3D/tree/master/examples/jupyter) examples.

## Using pyspark + notebook on a cluster

If you want to launch a pyspark from a cluster directly, you would use:

```bash
PYSPARK_DRIVER_PYTHON_OPTS="/path/to/jupyter-notebook --no-browser --port=7777" pyspark <...>
# --> Follow the URL, which includes the token
```
