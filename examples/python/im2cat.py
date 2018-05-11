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

import numpy as np
import pylab as pl

from astropy.stats import sigma_clipped_stats
from astropy.io import fits
from astropy.visualization import SqrtStretch
from astropy.visualization.mpl_normalize import ImageNormalize

from photutils import DAOStarFinder
from photutils import CircularAperture

import argparse

def addargs(parser):
    """ Parse command line arguments for readfits """

    ## Arguments
    parser.add_argument(
        '-inputpath', dest='inputpath',
        required=True,
        help='Path to a FITS file or a directory containing FITS files')

    ## Arguments
    parser.add_argument(
        '-hdustart', dest='hdustart',
        required=True,
        type=int,
        help='First HDU index to load.')

    parser.add_argument(
        '-hdustop', dest='hdustop',
        required=True,
        type=int,
        help='Last HDU index to load (included).')

    parser.add_argument(
        '-plot', dest='plot',
        default=False,
        action='store_true',
        help='Interactive plot if True. Default is False.')

    parser.add_argument(
        '-log_level', dest='log_level',
        default="ERROR",
        help='Level of log for Spark. Default is ERROR.')

def reshape_image(im):
    """
    By default, Spark shapes images into (nx, 1, ny).
    This routine reshapes images into (nx, ny)
    """
    shape = np.shape(im)
    return im.reshape((shape[0], shape[2]))

def rowdf_into_imagerdd(df, final_num_partition=1):
    """
    Reshape a DataFrame of rows into a RDD containing the full image
    in one partition.

    Parameters
    ----------
    df : DataFrame
        DataFrame of image rows.
    final_num_partition : Int
        The final number of partitions. Must be one (default) unless you
        know what you are doing.

    Returns
    ----------
    imageRDD : RDD
        RDD containing the full image in one partition
    """
    return df.rdd.coalesce(final_num_partition).glom()

def get_stat(data, sigma=3.0, iters=3):
    """
    Estimate the background and background noise using
    sigma-clipped statistics.

    Parameters
    ----------
    data : 2D array
        2d array containing the data.
    sigma : float
        sigma.
    iters : int
        Number of iteration to perform to get accurate estimate.
        The higher the better, but it will be longer.
    """
    mean, median, std = sigma_clipped_stats(data, sigma=sigma, iters=iters)
    return mean, median, std

def aggregate_ccd(args):
    """
    """
    df_init = spark.read\
        .format("com.sparkfits")\
        .option("hdu", args.hdustart)\
        .load(args.inputpath)
    imRDD = rowdf_into_imagerdd(df_init)

    for hdu in range(args.hdustart + 1, args.hdustop + 1):
        df = spark.read\
            .format("com.sparkfits")\
            .option("hdu", hdu)\
            .load(args.inputpath)
        imRDD = imRDD.union(rowdf_into_imagerdd(df))

    return imRDD

def quiet_logs(sc, log_level="ERROR"):
    """
    Set the level of log in Spark.

    Parameters
    ----------
    sc : SparkContext
        The SparkContext for the session
    log_level : String [optional]
        Level of log wanted: INFO, WARN, ERROR, OFF, etc.

    """
    ## Get the logger
    logger = sc._jvm.org.apache.log4j

    ## Set the level
    level = getattr(logger.Level, log_level, "INFO")

    logger.LogManager.getLogger("org"). setLevel(level)
    logger.LogManager.getLogger("akka").setLevel(level)


if __name__ == "__main__":
    """
    Read the data from a FITS file using Spark,
    and show the first rows and the schema.
    """
    parser = argparse.ArgumentParser(
        description='Distribute the data of a FITS file.')
    addargs(parser)
    args = parser.parse_args(None)

    spark = SparkSession\
        .builder\
        .getOrCreate()

    ## Set logs to be quiet
    quiet_logs(spark.sparkContext, log_level=args.log_level)

    imRDD = aggregate_ccd(args)

    ## Source detection: build the catalogs for each CCD in parallel
    cat = imRDD.map(
        lambda im: reshape_image(np.array(im)))\
        .map(
            lambda im: (im, get_stat(im)))\
        .map(
            lambda im_stat: (
                im_stat[0],
                im_stat[1][1],
                DAOStarFinder(fwhm=3.0, threshold=5.*im_stat[1][2])))\
        .map(
            lambda im_mean_starfinder: im_mean_starfinder[2](
                im_mean_starfinder[0] - im_mean_starfinder[1]))

    ## Collect on the driver the catalogs with all detected sources
    ## Should not be too big though!
    final_cat = cat.collect()
    print(final_cat)

    ## Loop over CCD catalogs
    for hdu in range(1, args.hdustop + 1):
        ## Grab initial data for plot
        data = fits.open(args.inputpath)
        data = data[hdu].data

        ## Plot the result on the CCD
        positions = (
            final_cat[hdu-1]['xcentroid'], final_cat[hdu-1]['ycentroid'])
        apertures = CircularAperture(positions, r=4.)
        norm = ImageNormalize(stretch=SqrtStretch())
        pl.imshow(data, cmap='Greys', origin="lower", norm=norm)
        apertures.plot(color='blue', lw=1.0, alpha=0.5)
        pl.savefig("CCD_{}.pdf".format(hdu))

        ## Plot if wanted.
        if args.plot:
            pl.show()
        pl.clf()
