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
