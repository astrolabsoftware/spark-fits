#!/bin/bash

# Package it
#sbt package
sbt assembly

# Jars
FITS=lib/nom-tam-fits-1.15.2.jar

# Parameters
fitsfn="/Users/julien/Documents/workspace/myrepos/ScalaTest/gridtest/data/cat29572.fits"
# fitsfn="/Users/julien/Documents/workspace/myrepos/ScalaTest/healpix/data/des_simbad_ra-dec-red_100000.fits"
#fitsfn="/Users/julien/Documents/Workspace_postdoc/Planck/COM_PCCS_143_R2.01.fits"
verbosity=false

# Run it!
spark-submit \
  --master local[*] \
  --class com.sparkfits.ReadFits \
  target/scala-2.11/spark-fits-assembly-0.1.0.jar \
  $fitsfn $verbosity

#target/scala-2.11/toto_2.11-0.1.0.jar
