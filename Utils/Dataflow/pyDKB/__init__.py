"""
Common library for Data Knowledge Base development.
"""

import dataflow
import common

import os


basedir = os.path.dirname(__file__)
with open(os.path.join(basedir, 'VERSION')) as version_file:
    __version__ = version_file.read().strip()

__all__ = ["dataflow"]
