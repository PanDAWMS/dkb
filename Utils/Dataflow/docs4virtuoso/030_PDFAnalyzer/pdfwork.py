#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PDF Analyzer script for extracting text from PDF with PDF Miner
"""

import sys
from tempfile import TemporaryFile

from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfdevice import PDFDevice
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfpage import PDFTextExtractionNotAllowed
from pdfminer.converter import XMLConverter, HTMLConverter, TextConverter
from pdfminer.layout import LAParams


def remove_ligatures(text):
    """ Replace ligatures with corresponding letter combinations.

    Ligatures are fancy symbols combining several letters.
    """
    text = text.replace("ﬃ", "ffi")
    text = text.replace("ﬁ", "fi")
    text = text.replace("ﬀ", "ff")
    text = text.replace("ﬂ", "fl")
    return text


def get_page_text(interpreter, page, tmp, rotation=0):
    """ Extract text from a page with given rotation.

    These variables are required and must be setup in parent function:
    interpreter - interpreter used by PDFMiner.
    page - page object.
    tmp - temporary file use to get processed text. Stack Overflow
    advices to create a new tmp file each time, but it must be specified
    when creating PDFMiner device object, so i'm not sure if that's
    possible in this case.
    """
    page.rotate = (page.rotate + rotation) % 360
    interpreter.process_page(page)
    tmp.seek(0)
    text = remove_ligatures(tmp.read())
    tmp.seek(0)
    tmp.truncate()
    return text


def mine_text(infname, page_numbers=False, outtype="text", rotated_pages=[],
              folder=False):
    """ Mine text from a PDF files.

    Rotated pages are found if output is txt, otherwise pages are
    rotated according to respective variable.
    """
    with open(infname, "rb") as inf:

        rsrcmngr = PDFResourceManager()

        tmp = TemporaryFile(mode="w+")
        laparams = LAParams()

        extension = False
        if outtype == "text":
            device = TextConverter(rsrcmngr, tmp, codec='utf-8',
                                   laparams=laparams)
            extension = "txt"
            rotated_pages = []
        elif outtype == "xml":
            device = XMLConverter(rsrcmngr, tmp, codec='utf-8',
                                  laparams=laparams)
            extension = "xml"
        elif outtype == "html":
            device = HTMLConverter(rsrcmngr, tmp, codec='utf-8',
                                   laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmngr, device)

        n = 1
        for page in PDFPage.get_pages(inf):
            if not page_numbers or n in page_numbers:
                if outtype == "xml" and n in rotated_pages:
                    rotation = 90
                else:
                    rotation = 0
                text = get_page_text(interpreter, page, tmp, rotation)
                if outtype == "text":
                    single = 0
                    normal = 0
                    lines = text.split("\n")
                    for line in lines:
                        if not line.isspace():
                            line = line.strip()
                            if len(line) == 1:
                                single += 1
                            normal += 1
                    coef = float(single) / normal
                    if coef > 0.6:
                        rotated_pages.append(n)
                        text = get_page_text(interpreter, page, tmp, 90)
                if folder and extension:
                    with open(folder + "/%d.%s" % (n, extension), "w") as outf:
                        outf.write(text)
            n += 1
    device.close()
    tmp.close()
    return [n - 1, rotated_pages]
