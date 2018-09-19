#!/usr/bin/env python

import setuptools
import os

package = 'pyDKB'
basedir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(basedir, package, 'VERSION')) as version_file:
    version = version_file.read().strip()

setuptools.setup(name=package,
                 version=version,
                 packages=setuptools.find_packages(),
                 package_data={'pyDKB.dataflow': ['dkbID.conf'],
                               'pyDKB': ['VERSION']})
