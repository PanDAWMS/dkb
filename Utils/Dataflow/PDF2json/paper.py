#!/bin/env python

'''
'''

import json
import os
import re
import shutil


import pdfwork
from utils import log, path_join


class Paper:
    ''' Class represents a document which needs to be analyzed. '''
    attributes_metadata = ['fname', 'num_pages', 'rotated_pages']
    txt_dir_name = 'txt'
    xml_dir_name = 'xml'
    re_pdfname = re.compile(r"([^./]+)\.pdf$")

    def __init__(self, dirname, fname=False, hdfs=False):
        self.dir = dirname
        self.txt_dir = path_join(self.dir, self.txt_dir_name)
        self.xml_dir = path_join(self.dir, self.xml_dir_name)
        self.metadata_file = path_join(self.dir, 'metadata.json')
        # This flag is set to True when part of metadata is changed, but
        # not yet saved to the metadata file.
        self.changed = False

        if fname:
            # Create a new paper from given file.
            # Create a directory.
            if os.access(self.dir, os.F_OK):
                log('Cannot create directory %s: filename already exists.'
                    % (self.dir), 'ERROR')
                return None
            os.mkdir(self.dir)

            # Copy the PDF file into the directory.
            if hdfs:
                raise NotImplementedError('Processing HDFS documents not '
                                          'implemented yet.')
            else:
                shutil.copy(fname, self.dir)

            self.fname = self.re_pdfname.search(fname).group(1)
            self.pdf = path_join(self.dir, self.fname + '.pdf')

            self.mine_text()

            self.save_metadata()
        else:
            # Create a paper object from existing directory.
            self.load_metadata()
            self.pdf = path_join(self.dir, self.fname + '.pdf')

    def get_txt_page(self, number, text=False):
        ''' Fetch txt page of the paper by number.

        Result is either text(if text variable is True) or lines (if
        text variable is False).
        '''
        fname = path_join(self.txt_dir, '%d.txt' % number)
        with open(fname) as f:
            if text:
                r = f.read()
            else:
                r = f.readlines()
        return r

    def get_xml_page(self, number, text=False):
        ''' Fetch xml page of the paper.

        Xml page is extracted from PDF if it was not done yet.
        Result is either text(if text variable is True) or
        lines (if text variable is False).
        '''
        fname = path_join(self.xml_dir, "%d.xml" % number)
        if not os.access(fname, os.F_OK):
            [num_pages, rotated_pages] = pdfwork.mine_text(self.pdf, [number],
                                                           'xml',
                                                           self.rotated_pages,
                                                           self.xml_dir)
        with open(fname) as f:
            if text:
                r = f.read()
            else:
                r = f.readlines()
        return r

    def mine_text(self):
        ''' Extract text from the PDF file. '''
        if not os.access(self.txt_dir, os.F_OK):
            os.mkdir(self.txt_dir)
        [num_pages,
         self.rotated_pages] = pdfwork.mine_text(self.pdf, folder=self.txt_dir)
        self.num_pages = num_pages
        if not os.access(self.xml_dir, os.F_OK):
            os.mkdir(self.xml_dir)
        self.get_xml_page(1)
        self.save_metadata()

    def get_text(self):
        ''' Read and return mined text of the document. '''
        text = ''
        for i in range(1, self.num_pages + 1):
            with open(path_join(self.txt_dir, '%d.txt') % i) as f:
                text += f.read()
        return text

    def save_metadata(self):
        ''' Save metadata to the metadata file. '''
        outp = {}
        for key in self.attributes_metadata:
            outp[key] = self.__dict__[key]
        with open(self.metadata_file, "w") as f:
            json.dump(outp, f, indent=4)
        self.changed = False

    def load_metadata(self):
        ''' Import metadata from a file. '''
        if not os.access(self.metadata_file, os.R_OK):
            return 0
        with open(self.metadata_file) as f:
            inp = json.load(f)
        for key in self.attributes_metadata:
            if key in inp:
                self.__dict__[key] = inp[key]

    def delete(self):
        ''' Delete all files associated with paper. '''
        shutil.rmtree(self.dir)
