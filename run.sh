#!/bin/bash

# Package it
sbt assembly

# Jars
FITS=lib/nom-tam-fits-1.15.2.jar

# Parameters (put your file)
fitsfn="src/test/resources/test.fits"

# Run it!
spark-submit \
  --master local[*] \
  --class com.sparkfits.ReadFits \
  target/scala-2.11/spark-fits-assembly-0.1.0.jar \
  $fitsfn
