"""
This subdirectory contains FITS test files.
---------------------------------------------------------------------
tst0001.fits   Simple 8-bit integer ramp
tst0002.fits   Simple 16-bit integer file with 3 axes 
tst0003.fits   Simple 32-bit integer file
tst0004.fits   Simple 32-bit integer file with scaling
tst0005.fits   Simple 32-bit IEEE Floating Point file
tst0006.fits   Simple 64-bit IEEE Floating Point file
tst0007.fits   Test file with 32-bit IEEE Fp special values
tst0008.fits   Test file with 64-bit IEEE Fp special values
tst0009.fits   ASCII Table extension  + IMAGE extension
test64bit1.fits
varitab.fits
"""

import unittest, os, numpy as n
import pfits

class TestFITS(unittest.TestCase):
    def testopenfile(self):
        f = pfits.FITS('tst0001.fits')
        self.assertRaises(ValueError, pfits.FITS, 'new.fits')
        self.assertRaises(ValueError, pfits.FITS, 'new.fits', 'w')
        self.assertRaises(ValueError, pfits.FITS, 'tst0001.fits', 'x')
        f = pfits.FITS('new.fits','c')
        del(f)
        os.remove('new.fits')
    def testnomakinghdus(self):
        self.assertRaises(RuntimeError, pfits.HDU)

class TestHDU(unittest.TestCase):
    def setUp(self):
        f = pfits.FITS('tst0001.fits')
        self.h = f.get_hdus()[0]
    def testtype(self):
        self.assertEqual(self.h.type, 'IMAGE_HDU')
    def testcards(self):
        self.assertEqual(len(self.h.cards), 25)
        for c in self.h.cards:
            self.assertEqual(len(c), 3)
            self.assertEqual(type(c[0]), str)
            self.assertEqual(type(c[1]), str)
            self.assertEqual(type(c[2]), str)
    def testkeys(self):
        keys = [k[0] for k in self.h.cards 
            if type(k[0]) is str and len(k[0]) > 0]
        self.assertEqual(self.h.keys(), keys)
    def testhaskey(self):
        for k in self.h.keys(): self.assertTrue(self.h.has_key(k))
        self.assertFalse(self.h.has_key('bug'))
    def testmapping(self):
        self.assertRaises(KeyError, lambda: self.h[[]])
        self.assertEqual(len(self.h), 25)
        for k in self.h.keys(): self.assertEqual(type(self.h[k]), str)
        self.assertRaises(KeyError, lambda: self.h['bug'])
    def testrepr(self):
        self.assertTrue(len(str(self.h)) > 0)
    def testgetdata(self):
        dat = self.h.get_data()
        self.assertEqual(dat.shape, (123,321))
        self.assertEqual(dat.dtype, n.ubyte)

class Test0001(unittest.TestCase):
    def setUp(self):
        f = pfits.FITS('tst0001.fits')
        self.hdus = f.get_hdus()
        self.h = self.hdus[0]
    def testheader0001(self):
        h = self.h
        self.assertEqual(len(h), 25)
        self.assertEqual(h['SIMPLE'],   "T")
        self.assertEqual(h['BITPIX'],   "8")
        self.assertEqual(h['NAXIS'],    "2")
        self.assertEqual(h['NAXIS1'],   "123")
        self.assertEqual(h['NAXIS2'],   "321")
        self.assertEqual(h['EXTEND'],   "T")
        self.assertEqual(h['BLOCKED'],  "T")
        self.assertEqual(h['CDELT1'],   "-2.3")
        self.assertEqual(h['CRVAL1'],   "-73.3")
        self.assertEqual(h['CRPIX1'],   "12.0")
        self.assertEqual(h['CTYPE1'],   "PIXEL")
        self.assertEqual(h['CDELT2'],   "7.1")
        self.assertEqual(h['CRVAL2'],   "300.1")
        self.assertEqual(h['CRPIX2'],   "-11.0")
        self.assertEqual(h['CTYPE2'],   "PIXEL")
        self.assertEqual(h['OBJECT'],   "Ramp 8-bit")
        self.assertEqual(h['ORIGIN'],   "ESO")
        self.assertEqual(h['DATE'],     "19/08/92")
    def testdata0001(self):
        dat = self.hdus[0].get_data()
        self.assertEqual(dat.shape, (123,321))
        self.assertEqual(dat.dtype, n.ubyte)
        ans = n.arange(1, dat.size+1) % 2**8
        ans.shape = dat.shape
        self.assertTrue(n.all(dat == ans))

class Test0002(unittest.TestCase):
    def setUp(self):
        f = pfits.FITS('tst0002.fits')
        self.hdus = f.get_hdus()
    def testdata0002(self):
        dat = self.hdus[0].get_data()
        self.assertEqual(dat.shape, (73,31,5))
        self.assertEqual(dat.dtype, n.short)
        ans = n.arange(0, dat.size, dtype=dat.dtype) % 73
        ans.shape = dat.shape
        self.assertTrue(n.all(dat == ans))

class Test0005(unittest.TestCase):
    def setUp(self):
        f = pfits.FITS('tst0005.fits')
        self.hdus = f.get_hdus()
    def testdata0005(self):
        dat = self.hdus[0].get_data()
        self.assertEqual(dat.shape, (102,109))
        self.assertEqual(dat.dtype, n.float32)
        self.assertTrue(n.all(n.abs(dat) <= 136))

class Test0007(unittest.TestCase):
    def setUp(self):
        f = pfits.FITS('tst0007.fits')
        self.hdus = f.get_hdus()
    def testdata0007(self):
        dat = self.hdus[0].get_data()
        self.assertEqual(dat.shape, (38,))
        self.assertEqual(dat.dtype, n.float32)
        self.assertTrue(n.any(n.isnan(dat)))

class Test0008(unittest.TestCase):
    def setUp(self):
        f = pfits.FITS('tst0008.fits')
        self.hdus = f.get_hdus()
    def testdata0008(self):
        dat = self.hdus[0].get_data()
        self.assertEqual(dat.shape, (38,))
        self.assertEqual(dat.dtype, n.double)
        self.assertTrue(n.any(n.isnan(dat)))

class Test0009(unittest.TestCase):
    def setUp(self):
        f = pfits.FITS('tst0009.fits')
        self.hdus = f.get_hdus()
    def testheader0009(self):
        self.assertEqual(len(self.hdus[0]), 11)
        self.assertEqual(len(self.hdus[1]), 63)
        self.assertEqual(len(self.hdus[2]), 32)
        for h in self.hdus:
            for k in h.keys():
                self.assertTrue(h.has_key(k))
                self.assertEqual(type(h[k]), str)
    def testhdu0data0009(self):
        self.assertEqual(self.hdus[0].type, 'IMAGE_HDU')
        dat = self.hdus[0].get_data()
        self.assertEqual(dat, None)
    def testhdu1data0009(self):
        self.assertEqual(self.hdus[1].type, 'ASCII_TBL')
        dat = self.hdus[1].get_data()
        self.assertEqual(dat['IDENT'].shape, (53,))
        self.assertEqual(dat['Mag'].shape, (53,))
        self.assertEqual(dat['Channel'].shape, (53,))
        self.assertEqual(dat['Dist'].shape, (53,))
        self.assertEqual(dat['Mass'].shape, (53,))
        self.assertEqual(dat['Class'].shape, (53,))
        self.assertEqual(dat['Type'].shape, (53,))
        self.assertEqual(dat['Class_No'].shape, (53,))
    def testhdu2data0009(self):
        self.assertEqual(self.hdus[2].type, 'IMAGE_HDU')
        dat = self.hdus[2].get_data()
        self.assertEqual(dat.shape, (73,31,5))
        self.assertEqual(dat.dtype, n.short)
        ans = n.arange(0, dat.size, dtype=dat.dtype) % 73
        ans.shape = dat.shape
        self.assertTrue(n.all(dat == ans))

class TestVARITAB(unittest.TestCase):
    def setUp(self):
        f = pfits.FITS('varitab.fits')
        self.hdus = f.get_hdus()
    def testheader_varitab(self):
        self.assertEqual(len(self.hdus[0]), 80)
        self.assertEqual(len(self.hdus[1]), 39)
        for h in self.hdus:
            for k in h.keys():
                self.assertTrue(h.has_key(k))
                self.assertEqual(type(h[k]), str)
    def testhdu1_varitab(self):
        self.assertEqual(self.hdus[1].type, 'BINARY_TBL')
        d = self.hdus[1].get_data()
        for i,row in enumerate(d['Avalue']):
            if i == 0: self.assertEqual(row, ' ')
            else: self.assertEqual(row, 'abcdefghijklmnopqrst'[:i+1])
        for i,row in enumerate(d['Lvalue']):
            ans = n.zeros(i+1, dtype=n.bool)
            try:
                for j in [1,4,5,9,10,11,16,17,18,19]: ans[j] = 1
            except(IndexError): pass
            if i > 0: ans[-2] = 0
            self.assertTrue(n.all(row == ans))
        for i,row in enumerate(d['Xvalue']):
            self.assertTrue(n.all(row <= 255))
            self.assertTrue(n.all(row >= 0))
        for i,row in enumerate(d['Bvalue']):
            ans = n.arange(1, i+2, dtype=n.int)
            if i > 0:
                # Should this be 88, or my own Nan value of 0?
                self.assertEqual(row[-2], 88)
                ans[-2] = 88
            self.assertTrue(n.all(row == ans))
        for i,row in enumerate(d['Ivalue']):
            ans = n.arange(1, i+2, dtype=n.int)
            if i > 0:
                # Should this be 88, or my own Nan value of 0?
                self.assertEqual(row[-2], 88)
                ans[-2] = 88
            self.assertTrue(n.all(row == ans))
        for i,row in enumerate(d['Jvalue']):
            ans = n.arange(1, i+2, dtype=n.int)
            if i > 0:
                # Should this be 88, or my own Nan value of 0?
                self.assertEqual(row[-2], 88)
                ans[-2] = 88
            self.assertTrue(n.all(row == ans))
        for i,row in enumerate(d['Evalue']):
            ans = n.arange(1, i+2, dtype=n.double)
            if i > 0:
                self.assertTrue(n.isnan(row[-2]))
                row[-2] = i
            self.assertTrue(n.all(row == ans))
        for i,row in enumerate(d['Dvalue']):
            ans = n.arange(1, i+2, dtype=n.double)
            if i > 0:
                self.assertTrue(n.isnan(row[-2]))
                row[-2] = i
            self.assertTrue(n.all(row == ans))
        for i,row in enumerate(d['Cvalue']):
            self.assertEqual(row.dtype, n.complex64)
            self.assertEqual(row.shape, (0,))
        for i,row in enumerate(d['Mvalue']):
            self.assertEqual(row.dtype, n.complex128)
            self.assertEqual(row.shape, (0,))
        
class Test64bit1(unittest.TestCase):
    def setUp(self):
        f = pfits.FITS('test64bit1.fits')
        self.hdus = f.get_hdus()
    def testheader_test64bit1(self):
        self.assertEqual(len(self.hdus[0]), 9)
        self.assertEqual(len(self.hdus[1]), 14)
        self.assertEqual(len(self.hdus[2]), 14)
        for h in self.hdus:
            for k in h.keys():
                self.assertTrue(h.has_key(k))
                self.assertEqual(type(h[k]), str)
    def testhdu0_test64bit1(self):
        d = self.hdus[0].get_data()
        self.assertEqual(d.dtype, n.longlong)
        self.assertEqual(d.shape, (5,3))
        ans = n.array(
            [[                  -1,-9223372036854775807,-9223372036854775806],
             [-9223372036854775805,-9223372036854775804,                  -2],
             [                  -1,                   0,                   1],
             [                   2, 9223372036854775803, 9223372036854775804],
             [ 9223372036854775805, 9223372036854775806,9223372036854775807]])
        self.assertTrue(n.all(d == ans))
    def testhdu1_test64bit1(self):
        d = self.hdus[1].get_data()
        for i,row in enumerate(d['Row']):
            self.assertEqual(i+1, row)
        ans = [-9223372036854775808,-9223372036854775807,-9223372036854775806,
            -9223372036854775805,-9223372036854775804,                  -2,
                              -1,                   0,                   1,
                               2, 9223372036854775803, 9223372036854775804,
             9223372036854775805, 9223372036854775806,9223372036854775807]
        for i,row in enumerate(d['Value']):
            self.assertEqual(row, ans[i])
    def testhdu1_test64bit1(self):
        d = self.hdus[2].get_data()
        for i,row in enumerate(d['Row']):
            self.assertEqual(i+1, row)
        ans = n.array(
           [-9223372036854775808,-9223372036854775807,-9223372036854775806,
            -9223372036854775805,-9223372036854775804,                  -2,
                              -1,                   0,                   1,
                               2, 9223372036854775803, 9223372036854775804,
             9223372036854775805, 9223372036854775806,9223372036854775807])
        for i,row in enumerate(d['Value']):
            self.assertTrue(n.all(row == ans[:i+1]))

if __name__ == '__main__':
    unittest.main()
