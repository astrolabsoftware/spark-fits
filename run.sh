#!/bin/bash

# Package it
sbt package

# Jars
FITS=lib/nom-tam-fits-1.15.2.jar

# Parameters
# fitsfn="/Users/julien/Documents/workspace/myrepos/ScalaTest/gridtest/data/cat29572.fits"
fitsfn="/Users/julien/Documents/workspace/myrepos/ScalaTest/healpix/data/des_simbad_ra-dec-red_100000.fits"
# Run it!
spark-submit \
  --master local[*] \
  --jars ${FITS} \
  target/scala-2.11/toto_2.11-0.1.0.jar \
  $fitsfn
