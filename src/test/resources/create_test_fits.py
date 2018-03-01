#
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
from __future__ import division, absolute_import, print_function
from astropy.io import fits
import numpy as np

import argparse

def addargs(parser):
    """ Parse command line arguments """

    ## Number of row
    parser.add_argument(
        '-nrow', dest='nrow',
        required=True,
        type=int,
        help='Number of row.')

    ## Output file name
    parser.add_argument(
        '-filename', dest='filename',
        default='test_file.fits',
        required=False,
        help='Name of the output file with .fits extension')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="""
            Create dummy FITS file for test purpose.
            To create a FITS file just run
                `python create_test_fits.py -nrow <number>`
            or
                `python create_test_fits.py -h`
            to get help.
            """)
    addargs(parser)
    args = parser.parse_args(None)

    ## Grab the number of row desired
    n = args.nrow

    ## Initialise the seed for random number generator
    state = np.random.RandomState(0)

    ## Primary HDU - just a header
    hdr = fits.Header()
    hdr['OBSERVER'] = "Toto l'asticot"
    hdr['COMMENT'] = "Here's some commentary about this FITS file."
    primary_hdu = fits.PrimaryHDU(header=hdr)

    ## First extension
    a1 = np.array(['NGC{:07d}'.format(i) for i in range(n)])
    a2 = np.array(state.rand(n) * 2 * np.pi)
    a3 = np.array(state.rand(n) * np.pi - np.pi / 2., dtype=np.float64)
    a4 = np.array(range(n), dtype=np.int64)
    a5 = np.ones(n, dtype=int)

    ## Create each column
    col1 = fits.Column(name='target', format='10A', array=a1)
    col2 = fits.Column(name='RA', format='E', array=a2)
    col3 = fits.Column(name='Dec', format='D', array=a3)
    col4 = fits.Column(name='Index', format='K', array=a4)
    col5 = fits.Column(name='RunId', format='J', array=a5)

    ## Format into columns
    cols = fits.ColDefs([col1, col2, col3, col4, col5])

    ## Make the first HDU.
    hdu1 = fits.BinTableHDU.from_columns(cols)

    ## Second extension
    a1 = np.array(['NGC{:07d}'.format(i) for i in range(n)])
    a2 = np.array(state.randint(0, 10*n, n))
    a3 = np.array(state.rand(n) > 0.5)

    ## Create each column
    col1 = fits.Column(name='target', format='20A', array=a1)
    col2 = fits.Column(name='Index', format='1J', array=a2)
    col3 = fits.Column(name='Discovery', format='L', array=a3)

    ## Format into columns
    cols = fits.ColDefs([col1, col2, col3])

    ## Make the second HDU.
    hdu2 = fits.BinTableHDU.from_columns(cols)

    ## Concatenate all HDU
    hdul = fits.HDUList([primary_hdu, hdu1, hdu2])

    ## Save on disk
    hdul.writeto(args.filename)
