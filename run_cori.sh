#!/bin/bash
#SBATCH -p debug
#SBATCH -N 6
#SBATCH -t 00:30:00
#SBATCH -J sparkFITS
#SBATCH -C haswell

module load spark
module load sbt

## SBT Version
SBT_VERSION=2.11.8
SBT_VERSION_SPARK=2.11

## Package version
VERSION=0.2.0

# Package it
sbt ++${SBT_VERSION} package

# Parameters (put your file)
fitsfn="/global/cscratch1/sd/<user>/<path>"

# Run it!
start-all.sh
spark-submit \
  --master $SPARKURL \
  --driver-memory 15G --executor-memory 20G --executor-cores 17 --total-executor-cores 102 \
  --class com.sparkfits.ReadFits \
  target/scala-${SBT_VERSION_SPARK}/spark-fits_${SBT_VERSION_SPARK}-${VERSION}.jar \
  $fitsfn
stop-all.sh
