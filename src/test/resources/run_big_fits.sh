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

for i in {27..31}; do
  num=$((2**i))
  echo "Process" $num "points"

  ## Create the FITS file
  python create_big_fits.py -nrow ${num} -filename fits_${num}.fits

  ## put the FITS file to HDFS
  hdfs dfs -put fits_${num}.fits

  ## Remove the file from the local FS
  rm fits_${num}.fits
done
