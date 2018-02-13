#!/bin/bash

SBT_VERSION=2.11.8
SBT_VERSION_SPARK=2.11

# Package it
sbt ++${SBT_VERSION} assembly

# Jars
FITS=lib/nom-tam-fits-1.15.2.jar

# Parameters (put your file)
fitsfn="src/test/resources/test.fits"

# Run it!
spark-submit \
  --master local[*] \
  --class com.sparkfits.ReadFits \
  target/scala-${SBT_VERSION_SPARK}/spark-fits-assembly-0.1.0.jar \
  $fitsfn
