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
SCALA_VERSION=2.11.8
SCALA_VERSION_SPARK=2.11

## Package version
VERSION=0.9.0

# Package it
sbt ++${SCALA_VERSION} package

# Parameters (put your file)
fitsfn="/global/cscratch1/sd/<user>/<path>"

# Run it!
start-all.sh
shifter spark-submit \
  --master $SPARKURL \
  --driver-memory 15g --executor-memory 50g --executor-cores 32 --total-executor-cores 192 \
  --jars target/scala-${SCALA_VERSION_SPARK}/spark-fits_${SCALA_VERSION_SPARK}-${VERSION}.jar \
  examples/python/readfits.py \
  -inputpath $fitsfn
stop-all.sh
