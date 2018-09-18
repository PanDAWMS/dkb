#!/usr/bin/env python

import setuptools

setuptools.setup(name='pyDKB',
                 packages=setuptools.find_packages(),
                 package_data={'pyDKB.dataflow': ['dkbID.conf']})
