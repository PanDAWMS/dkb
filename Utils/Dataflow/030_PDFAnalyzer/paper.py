# -*- coding: utf-8 -*-


import json
import os
import re
import shutil
import sys

import pdfwork
import xmltable
from dscategory import DatasetCategory


CONFIG_FILE = "config.json"
default_cfg = {
    "WORK_DIR": os.getcwd(),
    "DETERMINE_TITLE": False,
    "OPEN_INTERVALS_TEXT": False,
    "OPEN_INTERVALS_TABLES": False,
    "TABLES_IDS_ONLY": False,
    "HDFS_PDF_DIR": "",
    "HDFS_DOWNLOAD_COMMAND": "hadoop fs -get"
}


def load_config(default_cfg):
    save_needed = False
    try:
        with open(CONFIG_FILE, "r") as f:
            loaded_cfg = json.load(f)
    except Exception as e:
        sys.stderr.write("Exception while loading config: %s\n" % e)
        sys.stderr.write("No config file loaded, using default values"
                         "\n")
        loaded_cfg = {}
    cfg = {}
    for p in default_cfg:
        if p not in loaded_cfg:
            cfg[p] = default_cfg[p]
            save_needed = True
        else:
            cfg[p] = loaded_cfg[p]
    return cfg, save_needed


def save_config(cfg):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=4)


cfg, save_needed = load_config(default_cfg)
if save_needed:
    save_config(cfg)


def path_join(a, b):
    """ Wrapper around os.path.join.

    This wrapper is required to account for possible different
    separators in paths.
    """
    return os.path.join(a, b).replace("\\", "/")


def mask_intervals(text):
    """ Handle bracketed intervals.

    Cut out all bracketed intervals [...] from the text which can
    be present in the datasets names and replace them with
    INTERVALnumber! strings.
    """
    intervals = []
    m = True
    i = 0
    while m:
        m = re_interval.search(text)
        if m:
            text = text.replace(m.group(0), "INTERVAL%d!" % i)
            intervals.append(m.group(0))
            i += 1
    return text, intervals


def organize_intervals(intervals):
    """ Organize the datasets.

    Currently two changes are made:
    "[1/2/3]" string into ['1', '2', '3'] array
    "[9-12]" into ['09', '10', '11', '12'] array
    """
    ni = []
    for i in intervals:
        if "/" in i:
            ni.append(i.strip("[]").split("/"))
        elif "-" in i:
            (s, e) = i.strip("[]").split("-")
            if len(e) <= len(s):
                e = s[:-len(e)] + e
                if s <= e:
                    ni1 = [str(i1) for i1 in range(int(s), int(e) + 1)]
                    maxlen = len(max(ni1, key=lambda num: len(num)))
                    if len(min(ni1, key=lambda num: len(num))) != maxlen:
                        # TO DO: improve this.
                        ni2 = []
                        for i1 in ni1:
                            i2 = "0" * (maxlen - len(i1)) + i1
                            ni2.append(i2)
                        ni1 = ni2
                    ni.append(ni1)
    return ni


def process_diapason(d):
    """ Prepare diapasons for table processing.

    Transform a diapason string "X-Y" into a list
    [X, X+1, X+2, ..., Y-1, Y], or empty list if X > Y.
    """
    values = []
    (s, e) = d.split("-")
    if len(e) <= len(s):
        e = s[:-len(e)] + e
        if s <= e:
            values = [str(i) for i in range(int(s), int(e) + 1)]
    return values


if __name__ == "__main__":
    try:
        import Tkinter
        from tkFileDialog import askdirectory, askopenfilenames, asksaveasfile
        import tkMessageBox
    except Exception as e:
        sys.stderr.write("Exception while loading Tkinter: %s\n" % e)
        msg = "Tkinter and/or stuff related to it cannot be "\
            "loaded, graphical interface will not work\n"
        sys.stderr.write(msg)

    # Directory for papers' directories.
    PAPERS_DIR = path_join(cfg["WORK_DIR"], "papers")
    # Directory for exported files.
    EXPORT_DIR = path_join(cfg["WORK_DIR"], "export")
    # File for statistics about exported papers.
    STAT_FILE = path_join(EXPORT_DIR, "stat.csv")
    # File for errors during export.
    ERRORS_FILE = path_join(EXPORT_DIR, "errors.txt")
    # Font used for headings in the program.
    HEADING_FONT = ("Times New Roman", 20)

# Name of the subdirectory with txt files in a paper's directory.
TXT_DIR = "txt"
# Name of the subdirectory with xml files in a paper's directory.
XML_DIR = "xml"
# Name of the file which holds the metadata extracted from a paper.
METADATA_FILE = "metadata.json"


group = DatasetCategory("group", r"""
group              # Indicates group dataset.
\n*\.\n*           # Field separator
[a-zA-Z\d\-:]+     # Group name. Examples: phys-higgs, phys-beauty.
(\n*[._]\n*[a-zA-Z\d\-:!]+)+
                                     """)
user = DatasetCategory("user", r"""
user               # Indicates user dataset.
\n*\.\n*           # Field separator
[a-zA-Z\d\-:]+     # User name.
(\n*[._]\n*[a-zA-Z\d\-:!]+)+
                                   """)
montecarlo = DatasetCategory("montecarlo", r"""
mc\d\d             # Project. Examples: mc08, mc12.
\n*_\n*            # Field part separator
[a-zA-Z\d!]+       # Project sub tag. Examples: 7TeV, 1beam, cos.
\n*\.\n*           # Field separator
[\dINTERVAL!]+     # DataSet ID(DSID)
(\n*[._]\n*[a-zA-Z\d\-:!]+)+
(\n*_\n*[a-z]\d+)+ # AMITag or several
(_tid\d+(_\d\d)?)? # Possible production system task and subtask numbers
                                               """)
physcont = DatasetCategory("physcont", r"""
[a-zA-Z\d\-_\.:!]+
\n*\.\n*           # Field separator
PhysCont           # prodStep - it's always the same.
\n*\.\n*           # Field separator
[a-zA-Z\d\-_\.:!]+ #
\n*\.\n*           # Field separator
[t0proge]+\d\d_v?\d\d# version.
(\n*_\n*[a-z]\d+)* # Possible AMITag or several.
                                           """)
calibration = DatasetCategory("calibration", r"""
data\d\d_calib     # Project tag. Example: data08_calib.
\n*\.\n*           # Field separator
[\dINTERVAL!]+     # runNumber (8 digits) or timestamp (10 digits)
\n*\.\n*           # Field separator
[a-zA-Z\d\-_\.:!]+ #
\n*\.\n*           # Field separator
RAW                #
                                                 """)
realdata = DatasetCategory("realdata", r"""
data\d\d           # Project tag. Examples: data09, data10.
\n*_\n*            # Field part separator
[a-zA-Z\d!]+       # Project sub tag. Examples: 7TeV, 1beam, cos.
\n*\.\n*           # Field separator
[\dINTERVAL!]+     # runNumber (8 digits) or timestamp (10 digits)
\n*\.\n*           # Field separator
[a-zA-Z\d\-_\.:!]+ #
                                           """)
database = DatasetCategory("database", r"""
ddo                # Project tag.
\n*\.\n*           # Field separator
[\dINTERVAL!]+     #
\n*\.\n*           # Field separator
[a-zA-Z\d\-_\.:!]+ #
                                           """)

category_export_dict = {
    "group": "group",
    "user": "user",
    "montecarlo": "mc",
    "physcont": "cont",
    "calibration": "calib",
    "realdata": "real",
    "database": "db"
}


# Regular expressions
# We don't need group and user datasets for now.
dataset_categories = [montecarlo, physcont, calibration, realdata, database]
# Path must have / as separator, not \.
re_pdfname = re.compile(r"/([^./]+)\.pdf$")
re_table_caption = re.compile(r"Table \d+:.*?\n\n", re.DOTALL)
re_table_caption_short = re.compile(r"Table (\d+):")
re_table_datasets = re.compile("(?:sample|dataset|run)")
re_column_with_datasets = re.compile("^(?:d[cs]?[-_ ]?|mc[-_ ]?|data ?"
                                     "|dataset ?"
                                     "|period|request ?|run ?|sample ?)(?:id"
                                     "|number|period|range|sample|set)")
re_dsid = re.compile(r"^\d{4,8}$")
re_dsid_diap = re.compile(r"^\d{4,8}-\d{1,8}$")
re_xml_symbol = re.compile("^<text[^>]+ size=\"([0-9.]+)\">(.+)</text>$")
re_xml_empty_symbol = re.compile("^<text> </text>$")
re_atlas_name = re.compile(r"[A-Z0-9-]+-20\d\d-[A-Z0-9-]+")
re_campaign = re.compile(r"""(
                                mc11(?![abc])
                                |
                                mc11[abc]
                                |
                                mc12(?![ab])
                                |
                                mc12[ab]
                                |
                                pro1[045]
                                |
                                repro0[389]
                                |
                                repro1[4-9]
                                |
                                repro20
                                |
                                repro04_v1
                                |
                                repro05_v2
                                |
                                t0pro0[089]
                                |
                                t0pro1[1234579]
                                |
                                t0pro20
                                |
                                t0pro04_v1
                                |
                                t0proc03_v1
                                )""", re.X)
re_energy = re.compile(r"(\d+\.?\d*) (G|T)eV")
# WARNING: this "fb-1" is in UTF-8 coding and was copied from miner
# output. Simple "fb-1" does not work.
re_luminosity = re.compile(r"(\d+\.?\d*) ?(m|n|p|f)b(?:−|\(cid:0\))1")
re_collisions = re.compile("(proton-proton|heavy-ion|pp) collisions")
re_year = re.compile("(?:acquired|collected|measured|recorded).{0,100}?"
                     r"(20\d\d((\+|-| and )20\d\d)?)", re.DOTALL)
# Interval must contain at least two numbers, i.e. [1/2] or [3\4\5].
re_interval = re.compile(r"\[(?:[0-9][\\/][0-9\\/\n]+|[0-9]+-[0-9]+)\]")
re_link = re.compile(r"(.*)\n? ?(https?://cds\.cern\.ch/record/\d+)")



class Paper:
    """ Class represents a document which needs to be analyzed. """
    attributes_general = ["atlas_name", "campaigns", "energy", "luminosity",
                          "collisions", "data taking years",
                          "possible_project_montecarlo",
                          "possible_project_realdata", "links"]
    # Paper attributes which are needed but cannot be determined
    # precisely yet(unlike, for example, number of pages).
    attributes_to_determine = attributes_general + ["title", "datasets",
                                                    "datatables"]
    # Attributes which are saved / loaded to / from a file.
    attributes_metadata = attributes_to_determine + ["num_pages",
                                                     "rotated_pages"]

    def __init__(self, fname, dirname=False):
        self.fname = fname
        if not dirname:
            self.dir = path_join(PAPERS_DIR, self.fname)
        else:
            self.dir = dirname
        self.pdf = path_join(self.dir, "%s.pdf" % self.fname)
        self.txt_dir = path_join(self.dir, TXT_DIR)
        self.xml_dir = path_join(self.dir, XML_DIR)
        self.metadata_file = path_join(self.dir, METADATA_FILE)
        for a in self.attributes_to_determine:
            # This indicates that attributes should be determined when
            # need to display them arises for the first time. If nothing
            # was found, their values would be set to False or [] or {}.
            self.__dict__[a] = None

        # Number of pages in a paper.
        self.num_pages = None
        # Numbers of pages which are rotated.
        self.rotated_pages = None

        # This flag is set to True when part of metadata is changed, but
        # not yet saved to the metadata file.
        self.changed = False

    def get_txt_page(self, number, text=False):
        """ Fetch txt page of the paper.

        Result is either text(if text variable is True) or lines (if
        text variable is False).
        """
        fname = path_join(self.txt_dir, "%d.txt" % number)
        with open(fname, "r") as f:
            if text:
                r = f.read()
            else:
                r = f.readlines()
        return r

    def get_xml_page(self, number, text=False):
        """ Fetch xml page of the paper.

        Xml page is extract from PDF if it was not done yet.
        Result is either text(if text variable is True) or
        lines (if text variable is False).
        """
        fname = path_join(self.xml_dir, "%d.xml" % number)
        if not os.access(fname, os.F_OK):
            [num_pages, rotated_pages] = pdfwork.mine_text(self.pdf, [number],
                                                           "xml",
                                                           self.rotated_pages,
                                                           self.xml_dir)
        with open(fname, "r") as f:
            if text:
                r = f.read()
            else:
                r = f.readlines()
        return r

    def mine_text(self):
        """ Extract text from the PDF file. """
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
        """ Read and return mined text of the document. """
        text = ""
        for i in range(1, self.num_pages + 1):
            with open(path_join(self.txt_dir, "%d.txt") % i, "r") as f:
                text += f.read()
        return text

    def clear_metadata(self):
        """ Clear all non-precise document metadata. """
        for a in self.attributes_to_determine:
            if self.__dict__[a] is not None:
                self.changed = True
                self.__dict__[a] = None

    def save_metadata(self):
        """ Export metadata to a file. """
        outp = {}
        for key in self.attributes_metadata:
            outp[key] = self.__dict__[key]
        with open(self.metadata_file, "w") as f:
            json.dump(outp, f, indent=4)
        self.changed = False

    def load_metadata(self):
        """ Import metadata from a file. """
        if not os.access(self.metadata_file, os.R_OK):
            return 0
        with open(self.metadata_file, "r") as f:
            inp = json.load(f)
        for key in self.attributes_metadata:
            if key in inp:
                self.__dict__[key] = inp[key]

    def delete(self):
        """ Delete all files associated with paper. """
        shutil.rmtree(self.dir)

    def find_attributes_general(self):
        """ Find general attributes in a document. """
        attrs = {}
        text = self.get_text()

        attrs["campaigns"] = list(set(re_campaign.findall(text.lower())))

        pages = self.get_txt_page(1, True) + self.get_txt_page(2, True)

        attrs["energy"] = False
        tmp = re_energy.search(pages)
        if tmp:
            attrs["energy"] = tmp.group(0)

        attrs["luminosity"] = False
        tmp = re_luminosity.search(pages)
        if tmp:
            attrs["luminosity"] = tmp.group(0).replace("−", "-").\
                replace("(cid:0)", "-")

        links = re_link.findall(pages)
        attrs["links"] = {}
        for (key, value) in links:
            attrs["links"][key] = value

        attrs["atlas_name"] = False
        tmp = re_atlas_name.search(pages)
        if tmp:
            attrs["atlas_name"] = tmp.group(0)

        pages = pages.lower()

        attrs["collisions"] = False
        tmp = re_collisions.search(pages)
        if tmp:
            attrs["collisions"] = tmp.group(1)
            if attrs["collisions"] == "pp":
                attrs["collisions"] = "proton-proton"

        attrs["data taking years"] = False
        tmp = re_year.search(text)
        if tmp:
            if len(tmp.group(1)) == 4:
                attrs["data taking years"] = [tmp.group(1)]
            else:
                # Construct a list of all years, for example,
                # [2015, 2016, 2017] for "2015-2017". The same result will
                # be produced for "2015 and 2017", but no papers with such
                # interruptions in data collection were encountered. It is
                # also presumed that years are listed in correct order -
                # "2015 and 2016", but not "2016 and 2015".
                start = int(tmp.group(1)[:4])
                end = int(tmp.group(1)[-4:])
                attrs["data taking years"] = []
                for i in range(start, end + 1):
                    attrs["data taking years"].append(str(i))
        if attrs["campaigns"] and attrs["energy"]:
            mcc = False
            for c in attrs["campaigns"]:
                if c.startswith("mc"):
                    mcc = c
                    break
            if mcc:
                nrg = attrs["energy"].replace(" ", "")
                attrs["possible_project_montecarlo"] = mcc + "_" + nrg
            else:
                attrs["possible_project_montecarlo"] = False
        else:
            attrs["possible_project_montecarlo"] = False

        if attrs["data taking years"] and attrs["energy"]:
            attrs["possible_project_realdata"] = []
            nrg = attrs["energy"].replace(" ", "")
            for year in attrs["data taking years"]:
                y = year[2:4]
                proj = "data%s_%s" % (y, nrg)
                attrs["possible_project_realdata"].append(proj)
        else:
            attrs["possible_project_realdata"] = False

        return attrs

    def find_datasets(self):
        """ Find datasets in text of the document. """
        text = self.get_text()
        text, intervals = mask_intervals(text)
        if cfg["OPEN_INTERVALS_TEXT"]:
            intervals = organize_intervals(intervals)
        datasets = {}

        for c in dataset_categories:
            (text, datasets) = c.find(text, intervals, datasets)
        return (text, datasets)

    def find_datatables(self):
        """ Find tables in the document which may contain datasets. """
        pages_with_tables = []
        captions_data = {}
        n = 1
        # Find pages containing table captions.
        while n <= self.num_pages:
            text = self.get_txt_page(n, True)
            page_captions = re_table_caption.findall(text)
            page_captions_data = {}
            # Among the captions find ones which may hint that their
            # tables contain datasets. Store these captions, their
            # numbers and their pages.
            for h in page_captions:
                if re_table_datasets.search(h.lower()):
                    num = int(re_table_caption_short.match(h).group(1))
                    page_captions_data[num] = h
            if page_captions_data:
                pages_with_tables.append(n)
                captions_data.update(page_captions_data)
            n += 1

        datatables = {}
        # Extract all tables from selected pages.
        for n in pages_with_tables:
            text = self.get_xml_page(n, True)
            tables = xmltable.get_tables_from_text(text)
            # Save captions and tables matching selected numbers and
            # having dataset-related columns.
            for table in tables:
                num = int(re_table_caption_short.match(table.caption).group(1))
                if num in captions_data:
                    data_column = -1
                    skip_first = False
                    # Save captions and tables matching selected numbers
                    # and having dataset-related columns.
                    for rnum in range(0, min(2, len(table.rows))):
                        for i in range(0, len(table.rows[rnum])):
                            txt = table.rows[rnum][i].text.lower()
                            if re_column_with_datasets.match(txt):
                                data_column = i
                                if rnum == 1:
                                    # This means that first row contains
                                    # some kind of caption, or rubbish,
                                    # or something else, and columns are
                                    # defined in the second one. First
                                    # one must be skipped in such case.
                                    skip_first = True
                                break
                        if data_column >= 0:
                            break
                    # TODO: insert check that dataset column contains
                    # mostly \d\d\d\d\d\d. Also: duplicate rows in case
                    # of diapasones.
                    if data_column >= 0:
                        rows = []
                        # Start at 1 instead of 0 because the first row
                        # (which defines columns) will not contain a
                        # proper dataset/run id.
                        rows_with_proper_id = 1
                        diaps = False
                        for row in table.rows:
                            if skip_first:
                                skip_first = False
                                continue
                            row = [line.text.strip() for line in row]
                            if re_dsid.match(row[data_column]):
                                rows_with_proper_id += 1
                            elif re_dsid_diap.match(row[data_column]):
                                rows_with_proper_id += 1
                                diaps = True
                            rows.append(row)
                        coef = float(rows_with_proper_id) / len(rows)
                        if coef >= 0.7 and coef <= 1:
                            if cfg["OPEN_INTERVALS_TABLES"] and diaps:
                                rows_new = []
                                for row in rows:
                                    r_dc = row[data_column]
                                    if re_dsid_diap.match(r_dc):
                                        values = process_diapason(r_dc)
                                        for v in values:
                                            row_new = list(row)
                                            row_new[data_column] = v
                                            rows_new.append(row_new)
                                    else:
                                        rows_new.append(row)
                                    rows = rows_new
                            if cfg["TABLES_IDS_ONLY"]:
                                ids = []
                                for row in rows[1:]:
                                    dsid = row[data_column]
                                    if dsid != "EMPTY":
                                        ids.append(dsid)
                                ids.sort()
                                data = " ".join(ids)
                            else:
                                data = rows
                            datatables[num] = (captions_data[num], data)

        return datatables

    def export(self, quick=False, outf=False):
        """ Export metadata into file in export directory.

        Quick export: if a part of metadata was never determined, the
        corresponding procedure would be used with all user interaction
        skipped.
        """

        outp = {}
        if not outf:
            outf = path_join(EXPORT_DIR, "%s.json" % self.fname)

        # Some applications processing exported data may discard the
        # filename but it must be preserved.
        outp["fname"] = self.fname

        if self.title is not None:
            outp["title"] = self.title

        outp["content"] = {}
        outp["content"]["plain_text"] = {}
        # All general attributes are determined together, so we can
        # check only one.
        if self.campaigns is not None:
            for a in self.attributes_general:
                outp["content"]["plain_text"][a] = self.__dict__[a]
        elif quick:
            attrs = self.find_attributes_general()
            for a in attrs:
                outp["content"]["plain_text"][a] = attrs[a]

        if self.datasets is not None:
            for category in self.datasets:
                outp["content"][category_export_dict[category]
                                + "_datasets"] = self.datasets[category]
        elif quick:
            (text, datasets) = self.find_datasets()
            for category in datasets:
                d = [name for [name, special] in datasets[category]]
                outp["content"][category_export_dict[category]
                                + "_datasets"] = d
        if self.datatables is not None:
            for num in self.datatables:
                if isinstance(self.datatables[num][1], str)\
                   or isinstance(self.datatables[num][1], unicode):
                    caption, ids = self.datatables[num]
                    data = [caption, ids.split()]
                else:
                    data = self.datatables[num]
                outp["content"]["table_" + str(num)] = data
        elif quick:
            tables = self.find_datatables()
            for num in tables:
                if isinstance(tables[num][1], str)\
                   or isinstance(tables[num][1], unicode):
                    caption, ids = tables[num]
                    data = [caption, ids.split()]
                else:
                    data = tables[num]
                outp["content"]["table_" + str(num)] = data
        if outp:
            with open(outf, "w") as f:
                json.dump(outp, f, indent=4)
        return outp