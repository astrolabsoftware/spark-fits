#!/bin/bash
#SBATCH -p debug
#SBATCH -N 6
#SBATCH -t 00:30:00
#SBATCH -J sparkFITS
#SBATCH -C haswell
#SBATCH --image=nersc/spark-2.3.0:v1

module load spark
module load sbt

## SBT Version
SBT_VERSION=2.11.8
SBT_VERSION_SPARK=2.11

## Package version
VERSION=0.7.0

# Package it
sbt ++${SBT_VERSION} package

# Parameters (put your file)
fitsfn="/global/cscratch1/sd/<user>/<path>"

# Run it!
start-all.sh
shifter spark-submit \
  --master $SPARKURL \
  --driver-memory 15g --executor-memory 50g --executor-cores 32 --total-executor-cores 192 \
  --class com.astrolabsoftware.sparkfits.ReadFits \
  target/scala-${SBT_VERSION_SPARK}/spark-fits_${SBT_VERSION_SPARK}-${VERSION}.jar \
  $fitsfn
stop-all.sh
