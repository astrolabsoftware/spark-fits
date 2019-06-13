#!/bin/bash
# Copyright 2018 Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## SBT Version
SCALA_VERSION=2.11.8
SCALA_VERSION_SPARK=2.11

## Package version
VERSION=0.8.3

# Package it
sbt ++${SCALA_VERSION} package

# Parameters (put your file)
fitsfn="file://$PWD/src/test/resources/test_file.fits"

# Run it!
spark-submit \
  --master local[*] \
  --class com.astrolabsoftware.sparkfits.ReadFits \
  target/scala-${SCALA_VERSION_SPARK}/spark-fits_${SCALA_VERSION_SPARK}-${VERSION}.jar \
  $fitsfn
