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
SBT_VERSION=2.11.8
SBT_VERSION_SPARK=2.11

## Package version
VERSION=0.2.0

# Package it
sbt ++${SBT_VERSION} package

# Parameters (put your file)
fitsfn="file://$PWD/src/test/resources/test_file.fits"
# fitsfn="file://$PWD/src/test/resources/fits_33554432.fits"
# fitsfn="file://$PWD/src/test/resources/test_longheader_file.fits"
# fitsfn="file://$PWD/src/test/resources/toTest/swift_events.fits"
# fitsfn="file://$PWD/src/test/resources/dir"
# fitsfn="file://$PWD/src/test/resources/dirNotOk"

# Run it!
spark-submit \
  --master local[*] \
  --jars target/scala-${SBT_VERSION_SPARK}/spark-fits_${SBT_VERSION_SPARK}-${VERSION}.jar \
  examples/python/readfits.py \
  -inputpath $fitsfn
