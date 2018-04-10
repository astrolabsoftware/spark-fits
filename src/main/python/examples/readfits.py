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
from pyspark.sql import SparkSession

import argparse

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def addargs(parser):
    """ Parse command line arguments for readfits """

    ## Defaults args - load instrument, scan and sky parameters
    parser.add_argument(
        '-inputpath', dest='inputpath',
        required=True,
        help='Path to a FITS file or a directory containing FITS files')


if __name__ == "__main__":
    """

    """
    parser = argparse.ArgumentParser(
        description='Distribute the data of a FITS file.')
    addargs(parser)
    args = parser.parse_args(None)

    spark = SparkSession\
        .builder\
        .getOrCreate()

    ## Set logs to be quiet
    # quiet_logs(spark.sqlContext)

    ## Loop over HDUs, and print a few rows with the schema.
    for i in range(1, 3):
        df = spark.read\
            .format("com.sparkfits")\
            .option("hdu", 1)\
            .load(args.inputpath)

        df.show()
        df.printSchema()
