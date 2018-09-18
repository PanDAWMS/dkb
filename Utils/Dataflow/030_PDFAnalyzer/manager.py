# -*- coding: utf-8 -*-

"""
PDF Analyzer main script
"""

import json
import os
import re
import shutil
import sys

import pdfwork
import xmltable

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


class DatasetCategory:
    """ Class representing a dataset category.

    Contains regular expressions and function for finding datasets.
    reg - Standard regular expression.
    reg_spaces - Same as self.reg, but with spaces instead of
    underscores. This is required because pdfminer sometimes reads
    underscores as spaces, especially in a document with small font
    size.
    reg_dashes - Same as self.reg, but with dashes instead of
    underscores. Does not works, probably because "-" is a special
    character, and should be "sole dash" or "backslash-dash"
    in different places of regular expressions.
    """

    def __init__(self, name, string):
        self.name = name
        self.reg = re.compile(string, re.X)
        self.reg_spaces = re.compile(string.replace("_", r"\ ")
                                     .replace(r"\w", "a-zA-Z0-9 "), re.X)

    def find(self, text, intervals, datasets):
        strings = []
        (results, text) = find_cut_reg(self.reg, text)
        strings += results
        (results, text) = find_cut_reg(self.reg_spaces, text)
        strings += results
        if strings:
            datasets[self.name] = []
        for s in strings:
            s = s.strip()
            if "INTERVAL" in s:
                if cfg["OPEN_INTERVALS_TEXT"]:
                    nums = re.findall(r"INTERVAL(\d+)!", s)
                    arr = []
                    for n in nums:
                        arr.append(len(intervals[int(n)]))
                    size = min(arr)
                    # TO DO: If some intervals are shorter then it
                    # should be raised as a warning somewhere...
                    for i in range(0, size):
                        ns = s
                        for n in nums:
                            ns = re.sub("INTERVAL" + n + "!",
                                        intervals[int(n)][i], ns)
                        if self.reg.match(ns):
                            datasets[self.name].append([ns, False])
                        elif self.reg_spaces.match(ns):
                            datasets[self.name].append([ns.replace(" ", "_"),
                                                        "spaces"])
                else:
                    res = 0
                    if self.reg.match(s):
                        res = 1
                    elif self.reg_spaces.match(s):
                        res = 2
                    for i in range(0, len(intervals)):
                        s = re.sub("INTERVAL%d!" % i, intervals[i], s)
                    if res == 1:
                        datasets[self.name].append([s, False])
                    elif res == 2:
                        datasets[self.name].append([s.replace(" ", "_"),
                                                    "spaces"])
            else:
                if self.reg.match(s):
                    datasets[self.name].append([s, False])
                elif self.reg_spaces.match(s):
                    datasets[self.name].append([s.replace(" ", "_"),
                                                "spaces"])
        return (text, datasets)


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


def find_cut_reg(reg, text):
    """ Find and remove patterns matching regular expression. """
    results = []
    f = True
    while f:
        f = reg.search(text)
        if f:
            text = text.replace(f.group(0), "")
            results.append(f.group(0).replace("\n", ""))
    return (results, text)


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
                    ni1 = []
                    for i1 in range(int(s), int(e) + 1):
                        ni1.append(str(i1))
                    maxlen = len(max(ni1, key=lambda num: len(num)))
                    if len(min(ni1, key=lambda num: len(num))) != maxlen:
                        # TO DO: improve this.
                        ni2 = []
                        for i1 in ni1:
                            add_zeros = maxlen - len(i1)
                            i2 = ""
                            for j in range(0, add_zeros):
                                i2 += "0"
                            i2 += i1
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
            for i in range(int(s), int(e) + 1):
                values.append(str(i))
    return values


def check_all_button(v, lst):
    """ Command for handling Tkinter global checkbuttons.

    Command (un)checks all the checkbuttons in the list
    v - a VarInt variable associated with the global checkbutton.
    lst - a list of VarInt variables associated with checkbuttons in the
    list.
    """
    s = 0
    for i in lst:
        s += i.get()
    if s == len(lst):
        v.set(0)
        for i in lst:
            i.set(0)
    else:
        v.set(1)
        for i in lst:
            i.set(1)


def cmp_papernames(x, y):
    """ Compare paper names.

    Default cmp function thinks that, for example, "9" > "10"
    (it compares "9" and "1" first, and "9" > "1").
    """
    if x.isdigit() and y.isdigit():
        return int(x) - int(y)
    else:
        return cmp(x, y)


def scrollable_warning(parent, message, title="Warning"):
    """ Scrollable replacement for tkMessageBox.showwarning

    parent - parent window
    """
    window = Tkinter.Toplevel()
    window.title(title)
    window.wm_resizable(False, False)
    cnvs = Tkinter.Canvas(window)
    cnvs.grid(row=0, column=0)

    frame = Tkinter.Frame(cnvs)
    cnvs.create_window(0, 0, window=frame, anchor='nw')

    msg = Tkinter.Message(frame, text=message)
    msg.grid(row=0, column=0)

    scrlbr = Tkinter.Scrollbar(window, command=cnvs.yview)
    scrlbr.grid(row=0, column=2, rowspan=2, sticky='ns')
    cnvs.configure(yscrollcommand=scrlbr.set)
    frame.update_idletasks()
    rgn = (0, 0, frame.winfo_width(), frame.winfo_height())
    cnvs.configure(width=frame.winfo_width(), scrollregion=rgn)

    b = Tkinter.Button(window, text="Done",
                       command=window.destroy)
    b.grid(row=1, column=0)


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

        attrs["campaigns"] = []
        tmp = re_campaign.findall(text.lower())
        for c in tmp:
            attrs["campaigns"].append(c)
        attrs["campaigns"] = list(set(attrs["campaigns"]))

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
                d = []
                for [name, special] in datasets[category]:
                    d.append(name)
                outp["content"][category_export_dict[category]
                                + "_datasets"] = d
        if self.datatables is not None:
            for num in self.datatables:
                if isinstance(self.datatables[num][1], str)\
                   or isinstance(self.datatables[num][1], unicode):
                    caption, ids = self.datatables[num]
                    data = [caption, [i for i in ids.split()]]
                else:
                    data = self.datatables[num]
                outp["content"]["table_" + str(num)] = data
        elif quick:
            tables = self.find_datatables()
            for num in tables:
                if isinstance(tables[num][1], str)\
                   or isinstance(tables[num][1], unicode):
                    caption, ids = tables[num]
                    data = [caption, [i for i in ids.split()]]
                else:
                    data = tables[num]
                outp["content"]["table_" + str(num)] = data
        if outp:
            with open(outf, "w") as f:
                json.dump(outp, f, indent=4)
        return outp


class Manager:
    """ Main class of the application, performs most of the work. """

    def __init__(self, window):
        self.window = window
        self.window.title("Support notes manager")
        main_menu = Tkinter.Menu(self.window)
        papers_menu = Tkinter.Menu(main_menu, tearoff=0)
        export_menu = Tkinter.Menu(main_menu, tearoff=0)
        papers_menu.add_command(label="Add...", command=self.add_papers)
        papers_menu.add_command(label="Save all", command=self.save_paper)
        papers_menu.add_command(label="Clear all", command=self.clear_paper)
        papers_menu.add_command(label="Exit", command=self.finish)
        main_menu.add_cascade(label="Papers", menu=papers_menu)
        export_menu.add_command(label="Quick export",
                                command=lambda: self.export_all(quick=True))
        export_menu.add_command(label="Export", command=self.export_all)
        export_menu.add_command(label="Export texts",
                                command=self.export_texts)
        main_menu.add_cascade(label="Export", menu=export_menu)
        main_menu.add_command(label="Preferences", command=self.preferences)
        self.window.config(menu=main_menu)

        self.papers = []
        if not os.access(PAPERS_DIR, os.F_OK):
            os.mkdir(PAPERS_DIR)
        # Check papers directory and load papers from it.
        objs = os.listdir(PAPERS_DIR)
        for o in objs:
            if os.path.isdir(path_join(PAPERS_DIR, o)):
                p = Paper(o)
                p.load_metadata()
                self.papers.append(p)

        if not os.access(EXPORT_DIR, os.F_OK):
            os.mkdir(EXPORT_DIR)

        self.cnvs = Tkinter.Canvas(self.window, width=1200, height=800)
        self.cnvs.grid(row=1, column=0)
        self.frame = Tkinter.Frame(self.cnvs)
        self.cnvs.create_window(0, 0, window=self.frame, anchor='nw')
        scrlbr = Tkinter.Scrollbar(self.window, command=self.cnvs.yview)
        scrlbr.grid(row=0, rowspan=2, column=1, sticky='ns')
        self.cnvs.configure(yscrollcommand=scrlbr.set)

        self.status = Tkinter.Label(self.window, text="", bd=1,
                                    relief=Tkinter.SUNKEN)
        self.status.grid(row=2, sticky='we')

        # Intercept closing the program via Alt + F4 or other methods to
        # perform a clean exit.
        self.window.protocol("WM_DELETE_WINDOW", self.finish)

        self.redraw()
        self.window.mainloop()

    def unsaved_papers(self):
        """ Check if at least one paper was changed but not saved. """
        for p in self.papers:
            if p.changed:
                return True
        return False

    def finish(self):
        """ Exit application.

        Ask about saving the changes first if any are present.
        """
        msg = "Some papers were changed. Do you want to save these changes?"
        if self.unsaved_papers():
            if tkMessageBox.askyesno("Save changes?", msg):
                self.save_paper()
            self.window.destroy()
        else:
            self.window.destroy()

    def status_set(self, text=""):
        """ Update status bar. """
        self.status.configure(text=text)

    def redraw(self):
        """ Redraw the main window. """
        for c in self.frame.winfo_children():
            c.destroy()

        self.papers.sort(cmp=cmp_papernames, key=lambda paper: paper.fname)

        r = 0
        for p in self.papers:
            if p.changed:
                t = p.fname + "*"
            else:
                t = p.fname
            b = Tkinter.Button(self.frame, text=t)
            b.config(command=lambda paper=p:
                     self.show_paper_info(False, paper))
            b.grid(row=r, column=0)
            if p.title is not None:
                lbl = Tkinter.Label(self.frame, text=p.title)
                lbl.grid(row=r, column=1)
            r += 1

        self.frame.update_idletasks()
        self.cnvs.configure(scrollregion=(0, 0, self.frame.winfo_width(),
                                          self.frame.winfo_height()))

    def preferences(self):
        """ Show preferences window. """
        w = Tkinter.Toplevel()
        w.title("Preferences")
        w.transient(self.window)
        w.grab_set()
        determine_title = Tkinter.BooleanVar()
        determine_title.set(cfg["DETERMINE_TITLE"])
        open_intervals_text = Tkinter.BooleanVar()
        open_intervals_text.set(cfg["OPEN_INTERVALS_TEXT"])
        open_intervals_tables = Tkinter.BooleanVar()
        open_intervals_tables.set(cfg["OPEN_INTERVALS_TABLES"])
        tables_ids_only = Tkinter.BooleanVar()
        tables_ids_only.set(cfg["TABLES_IDS_ONLY"])
        work_dir = Tkinter.StringVar()
        work_dir.set(cfg["WORK_DIR"])

        frame = Tkinter.Frame(w)

        lbl = Tkinter.Label(frame, text="Working directory")
        lbl.grid(row=0, column=0)
        e = Tkinter.Entry(frame, width=100, textvariable=work_dir)
        e.grid(row=0, column=1)

        lbl = Tkinter.Label(frame, text="Determine papers' titles")
        lbl.grid(row=1, column=0)
        b = Tkinter.Checkbutton(frame, variable=determine_title)
        b.grid(row=1, column=1)

        lbl = Tkinter.Label(frame, text="Open intervals in text")
        lbl.grid(row=2, column=0)
        b = Tkinter.Checkbutton(frame, variable=open_intervals_text)
        b.grid(row=2, column=1)

        lbl = Tkinter.Label(frame, text="Open intervals in tables")
        lbl.grid(row=3, column=0)
        b = Tkinter.Checkbutton(frame, variable=open_intervals_tables)
        b.grid(row=3, column=1)

        txt = "Extract dataset IDs instead of full tables"
        lbl = Tkinter.Label(frame, text=txt)
        lbl.grid(row=4, column=0)
        b = Tkinter.Checkbutton(frame, variable=tables_ids_only)
        b.grid(row=4, column=1)

        frame.grid(row=0, column=0)
        b = Tkinter.Button(w, text="Done", command=w.destroy)
        b.grid(row=1, column=0)
        self.window.wait_window(w)

        restart = False
        if cfg["WORK_DIR"] != work_dir.get():
            restart = True
            cfg["WORK_DIR"] = work_dir.get()
        if determine_title.get():
            cfg["DETERMINE_TITLE"] = True
        else:
            cfg["DETERMINE_TITLE"] = False
        if open_intervals_text.get():
            cfg["OPEN_INTERVALS_TEXT"] = True
        else:
            cfg["OPEN_INTERVALS_TEXT"] = False
        if open_intervals_tables.get():
            cfg["OPEN_INTERVALS_TABLES"] = True
        else:
            cfg["OPEN_INTERVALS_TABLES"] = False
        if tables_ids_only.get():
            cfg["TABLES_IDS_ONLY"] = True
        else:
            cfg["TABLES_IDS_ONLY"] = False
        save_config(cfg)
        if restart:
            msg = "Program needs to be restarted to apply the changes."
            tkMessageBox.showwarning("Restart needed", msg)
            self.finish()

    def add_papers(self, fnames=None, errors=None, n=None):
        """ Add new papers from PDF files. """
        if fnames is None:
            fnames = askopenfilenames(initialdir=cfg["WORK_DIR"],
                                      filetypes=[("PDF files", ".pdf")])
            if not fnames:
                return False
            errors = {}
            n = 1
            self.window.after(100, lambda: self.add_papers(fnames, errors, n))
        else:
            f = fnames[n - 1]
            m = re_pdfname.search(f)
            if m:
                fname = m.group(1)
                for p in self.papers:
                    if p.fname == fname:
                        errors[p.fname] = "paper already exists."
                        fname = False
                        break
                if fname:
                    msg = "Extracting text %d/%d. Paper: %s. Please, wait..."\
                          % (n, len(fnames), fname)
                    self.status_set(msg)
                    self.window.update_idletasks()
                    paper_dir = path_join(PAPERS_DIR, m.group(1))
                    try:
                        os.mkdir(paper_dir)
                        shutil.copy(f, paper_dir)
                        p = Paper(m.group(1))
                        p.mine_text()
                        self.papers.append(p)
                    except Exception as e:
                        errors[fname] = e
                        if os.access(paper_dir, os.F_OK):
                            shutil.rmtree(paper_dir)
            if n < len(fnames):
                n += 1
                self.window.after(100, lambda: self.add_papers(fnames,
                                                               errors, n))
            else:
                self.status_set("")
                self.window.update_idletasks()
                if errors:
                    keys = errors.keys()
                    keys.sort(cmp=cmp_papernames)
                    msg = "These papers were not added for some reason:\n\n"
                    for e in keys:
                        msg += "%s : %s\n\n" % (e, errors[e])
                    scrollable_warning(self.window, msg,
                                       "Unable to add papers")
                self.redraw()

    def clear_paper(self, window=False, paper=False):
        """ Clear a single or all papers' metadata. """
        if paper:
            paper.clear_metadata()
        else:
            msg = "Are you sure you want to clear all papers' metadata?"
            if tkMessageBox.askyesno("Clear all metadata?", msg):
                for p in self.papers:
                    p.clear_metadata()
        if window:
            window.destroy()
        self.redraw()

    def save_paper(self, paper=False, finish=False):
        """ Save a single or all papers' metadata.

        Exit afterwards if finish is True.
        """
        if paper:
            paper.save_metadata()
        else:
            for p in self.papers:
                p.save_metadata()
        if finish:
            self.window.destroy()
        else:
            self.redraw()

    def delete_paper(self, window, paper):
        """ Delete paper. """
        if tkMessageBox.askyesno("Delete paper?",
                                 "Are you sure you want to delete paper %s?"
                                 % paper.fname):
            self.papers.remove(paper)
            paper.delete()
            if window:
                window.destroy()
            self.redraw()

    def show_paper_info(self, window, paper):
        """ Display information about a paper.

        Possible actions are also displayed.
        """
        if paper.title is None and cfg["DETERMINE_TITLE"]:
            self.determine_paper_title_step_1(window, paper)
            return 0
        if not window:
            window = Tkinter.Toplevel()
        else:
            for c in window.winfo_children():
                c.destroy()
        window.title("Info: %s" % paper.fname)
        lbl = Tkinter.Label(window, text="File name: %s" % paper.fname)
        lbl.grid(row=0, column=1)
        b = Tkinter.Button(window, text="Save",
                           command=lambda paper=paper: self.save_paper(paper))
        b.grid(row=0, column=2)
        # Maybe this should ask for save before export...
        b = Tkinter.Button(window, text="Export",
                           command=lambda paper=paper: paper.export())
        b.grid(row=0, column=3)
        b = Tkinter.Button(window, text="Clear", command=lambda window=window,
                           paper=paper: self.clear_paper(window, paper))
        b.grid(row=0, column=4)
        b = Tkinter.Button(window, text="Delete", command=lambda window=window,
                           paper=paper: self.delete_paper(window, paper))
        b.grid(row=0, column=5)
        lbl = Tkinter.Label(window, text="Title: %s" % paper.title)
        lbl.grid(row=1, columnspan=5)
        lbl = Tkinter.Label(window, text="Pages: %d" % paper.num_pages)
        lbl.grid(row=2, columnspan=5)
        lbl = Tkinter.Label(window, text="Rotated pages: %s" %
                            str(paper.rotated_pages))
        lbl.grid(row=3, columnspan=5)
        b = Tkinter.Button(window, text="Attributes",
                           command=lambda window=window,
                           paper=paper: self.show_paper_attributes(window,
                                                                   paper))
        b.grid(row=4, columnspan=5)
        b = Tkinter.Button(window, text="Datasets",
                           command=lambda window=window,
                           paper=paper: self.show_paper_datasets(window,
                                                                 paper))
        b.grid(row=5, columnspan=5)
        b = Tkinter.Button(window, text="Dataset tables",
                           command=lambda window=window,
                           paper=paper: self.show_paper_datatables(window,
                                                                   paper))
        b.grid(row=6, columnspan=5)
        b = Tkinter.Button(window, text="Export text",
                           command=lambda paper=paper:
                           self.export_paper_text(paper))
        b.grid(row=7, columnspan=5)
        b = Tkinter.Button(window, text="Close", command=window.destroy)
        b.grid(row=8, columnspan=5)

    def determine_paper_title_step_1(self, window, paper):
        """ Search the first xml page for a possible paper titles.

        User is asked to pick one. """
        if not window:
            window = Tkinter.Toplevel()
        else:
            for c in window.winfo_children():
                c.destroy()
        window.title("Determine paper title")
        lines = paper.get_xml_page(1)

        d = {}
        r = re.compile("^<text[^>]+ size=\"([0-9.]+)\">(.+)</text>$")
        empty = re.compile("^<text> </text>$")
        for line in lines:
            m = r.match(line)
            if m:
                size = m.group(1)
                text = m.group(2)
                if size in d.keys():
                    d[size] += text
                else:
                    d[size] = text
            elif empty.match(line):
                d[size] += " "

        msg = "Please, select a string which looks like a title of the paper"
        lbl = Tkinter.Label(window, text=msg, font=HEADING_FONT)
        lbl.grid(row=0, columnspan=2)
        r = 1
        selection = Tkinter.StringVar()
        for key in d:
            if len(d[key]) > 10:
                if r == 1:
                    selection.set(d[key])
                button = Tkinter.Radiobutton(window, text=d[key],
                                             variable=selection, value=d[key],
                                             wraplength=800)
                button.grid(row=r, columnspan=2)
                r += 1
        button = Tkinter.Button(window, text="Proceed",
                                command=lambda window=window, paper=paper,
                                ptv=selection:
                                self.determine_paper_title_step_2(window,
                                                                  paper,
                                                                  ptv))
        button.grid(row=r, column=0)
        button = Tkinter.Button(window, text="Cancel", command=window.destroy)
        button.grid(row=r, column=1)

    def determine_paper_title_step_2(self, window, paper,
                                     possible_title_variable):
        """ Construct a paper title from selected string. """
        for c in window.winfo_children():
            c.destroy()
        lines = paper.get_txt_page(1)

        possible_title = possible_title_variable.get()

        title = ""
        for line in lines:
            if len(line) <= 4 or line.startswith("Supporting Note")\
               or line.startswith("ATLAS NOTE"):
                continue
            words = line.split()
            i = 0
            for w in words:
                try:
                    # This throws exception sometimes, something about
                    # ascii codec unable to decode.
                    w_in = w in possible_title
                except Exception as e:
                    w_in = False
                if len(w) > 1 and w_in:
                    i += 1
            if i > 1 or (len(words) == 1 and i == 1):
                title += line.replace("\n", " ")
            elif title:
                break
        lbl = Tkinter.Label(window,
                            text="Please, correct the title, if needed.",
                            font=HEADING_FONT)
        lbl.grid(row=0, columnspan=2)
        e = Tkinter.Entry(window, width=150)
        e.insert(0, title)
        e.grid(row=1, columnspan=2)
        button = Tkinter.Button(window, text="Done",
                                command=lambda window=window, paper=paper,
                                value=e: self.update_paper_parameter(window,
                                                                     paper,
                                                                     "title",
                                                                     value))
        button.grid(row=2, column=0)
        button = Tkinter.Button(window, text="Back",
                                command=lambda window=window,
                                paper=paper:
                                self.determine_paper_title_step_1(window,
                                                                  paper))
        button.grid(row=2, column=1)

    def update_paper_parameter(self, window, paper, param, value):
        """ Update a part of paper metadata and display it. """
        if param == "title":
            paper.title = value.get()
            self.show_paper_info(window, paper)
        elif param == "general":
            for a in value:
                paper.__dict__[a] = value[a]
            self.show_paper_attributes(window, paper)
        elif param == "datasets":
            datasets = {}
            for c in value:
                datasets[c] = []
                for [entry, special, selected] in value[c]:
                    if selected.get():
                        # datasets[c].append([entry.get(), special])
                        # special is not needed. Maybe temporary.
                        datasets[c].append(entry.get())
                if not datasets[c]:
                    del datasets[c]
            if datasets:
                for c in datasets:
                    datasets[c].sort(key=lambda d: d[0])
            paper.datasets = datasets
            self.show_paper_datasets(window, paper)
        elif param == "datatables":
            paper.datatables = {}
            for [num, caption, data, selected] in value:
                if selected.get():
                    if isinstance(data, list):
                        paper.datatables[num] = (caption, data)
                    else:
                        paper.datatables[num] = (caption,
                                                 data.get("0.0",
                                                          "end").strip())
            self.show_paper_datatables(window, paper)
        paper.changed = True
        self.redraw()

    def show_paper_attributes(self, window, paper):
        """ Determine / display paper's general attributes. """
        for c in window.winfo_children():
            c.destroy()
        window.title("Attributes of %s" % paper.fname)
        if paper.campaigns is None:
            attrs = paper.find_attributes_general()
            self.update_paper_parameter(window, paper, "general", attrs)
        else:
            r = 0
            for a in paper.attributes_general:
                if a == "links":
                    if paper.__dict__[a]:
                        lbl = Tkinter.Label(window, text="Links:")
                        lbl.grid(row=r, column=0)
                        r += 1
                        for key in paper.__dict__[a]:
                            txt = "%s %s" % (key, paper.__dict__[a][key])
                            lbl = Tkinter.Label(window, text=txt)
                            lbl.grid(row=r, column=0)
                            r += 1
                    else:
                        lbl = Tkinter.Label(window, text="No links")
                        lbl.grid(row=r, column=0)
                        r += 1
                else:
                    txt = "%s:%s" % (a, str(paper.__dict__[a]))
                    lbl = Tkinter.Label(window, text=txt)
                    lbl.grid(row=r, column=0)
                    r += 1
            b = Tkinter.Button(window, text="Back",
                               command=lambda window=window,
                               paper=paper: self.show_paper_info(window,
                                                                 paper))
            b.grid(row=r, column=0)

    def show_paper_datasets(self, window, paper):
        """ Determine / display paper datasets. """
        for c in window.winfo_children():
            c.destroy()
        window.title("Datasets in %s" % paper.fname)
        if paper.datasets is None:
            (text, datasets) = paper.find_datasets()

            if datasets:
                cnvs = Tkinter.Canvas(window, width=1200, height=800)
                cnvs.grid(row=0, column=0, columnspan=2)

                frame = Tkinter.Frame(cnvs)
                cnvs.create_window(0, 0, window=frame, anchor='nw')

                r = 0
                dataset_entries = {}
                for c in datasets:
                    lbl = Tkinter.Label(frame, text=c, font=HEADING_FONT)
                    lbl.grid(row=r, column=0, columnspan=2)
                    c_s = Tkinter.IntVar()
                    c_s.set(1)
                    check_category_b = Tkinter.Checkbutton(frame, var=c_s)
                    check_category_b.grid(row=r, column=3)
                    r += 1
                    dataset_entries[c] = []
                    selected_list = []
                    datasets[c].sort(key=lambda d: d[0])
                    for [name, special] in datasets[c]:
                        lbl = Tkinter.Label(frame, text=special)
                        lbl.grid(row=r, column=0)
                        e = Tkinter.Entry(frame, width=150)
                        e.insert(0, name)
                        e.grid(row=r, column=1)
                        selected = Tkinter.IntVar()
                        selected.set(1)
                        b = Tkinter.Checkbutton(frame, var=selected)
                        # TO DO: checkbuttons for "(un)select all".
                        dataset_entries[c].append([e, special, selected])
                        selected_list.append(selected)
                        b.grid(row=r, column=3, pady=5)
                        r += 1
                    # This command is not called when individual
                    # checkbuttons are clicked - therefore, global
                    # checkbox will not change its look.
                    check_category_b.config(command=lambda v=c_s,
                                            lst=selected_list:
                                            check_all_button(v, lst))

                scrlbr = Tkinter.Scrollbar(window, command=cnvs.yview)
                scrlbr.grid(row=0, column=2, rowspan=2, sticky='ns')
                cnvs.configure(yscrollcommand=scrlbr.set)
                frame.update_idletasks()
                rgn = (0, 0, frame.winfo_width(), frame.winfo_height())
                cnvs.configure(width=frame.winfo_width(), scrollregion=rgn)

                b = Tkinter.Button(window, text="Done",
                                   command=lambda window=window, paper=paper,
                                   value=dataset_entries:
                                   self.update_paper_parameter(window, paper,
                                                               "datasets",
                                                               value))
                b.grid(row=1, column=0)
                b = Tkinter.Button(window, text="Cancel",
                                   command=lambda window=window,
                                   paper=paper:
                                   self.show_paper_info(window, paper))
                b.grid(row=1, column=1)
            else:
                datasets = {}
                self.update_paper_parameter(window, paper, "datasets",
                                            datasets)
        else:
            if not paper.datasets:
                lbl = Tkinter.Label(window, text="No datasets found",
                                    font=HEADING_FONT)
                lbl.grid(row=0)
            else:
                cnvs = Tkinter.Canvas(window, width=1200, height=800)
                cnvs.grid(row=0, column=0, columnspan=2)

                frame = Tkinter.Frame(cnvs)
                cnvs.create_window(0, 0, window=frame, anchor='nw')

                r = 0
                for k in paper.datasets:
                    lbl = Tkinter.Label(frame, text=k, font=HEADING_FONT)
                    lbl.grid(row=r)
                    r += 1
                    # for [d, special] in paper.datasets[k]:
                    # special is not needed. Maybe temporary.
                    for d in paper.datasets[k]:
                        lbl = Tkinter.Label(frame, text=d)
                        lbl.grid(row=r)
                        r += 1

                scrlbr = Tkinter.Scrollbar(window, command=cnvs.yview)
                scrlbr.grid(row=0, column=2, rowspan=2, sticky='ns')
                cnvs.configure(yscrollcommand=scrlbr.set)
                frame.update_idletasks()
                rgn = (0, 0, frame.winfo_width(), frame.winfo_height())
                cnvs.configure(width=frame.winfo_width(), scrollregion=rgn)
            b = Tkinter.Button(window, text="Back",
                               command=lambda window=window,
                               paper=paper: self.show_paper_info(window,
                                                                 paper))
            b.grid(row=1)

    def show_paper_datatables(self, window, paper):
        """ Determine / display paper tables with datasets. """
        for c in window.winfo_children():
            c.destroy()
        window.title("Tables with datasets in %s" % paper.fname)
        if paper.datatables is None:
            datatables = paper.find_datatables()
            if datatables:

                cnvs = Tkinter.Canvas(window, width=600, height=800)
                cnvs.grid(row=0, column=0, columnspan=2)

                frame = Tkinter.Frame(cnvs)
                cnvs.create_window(0, 0, window=frame, anchor='nw')

                num = 0
                keys = datatables.keys()
                keys.sort()
                datatables_s = []
                for k in keys:
                    (caption, data) = datatables[k]
                    t_frame = Tkinter.Frame(frame)
                    selected = Tkinter.IntVar()
                    selected.set(1)
                    lbl = Tkinter.Label(t_frame, text=caption,
                                        font=HEADING_FONT)
                    b = Tkinter.Checkbutton(t_frame, var=selected)
                    if isinstance(data, str) or isinstance(data, unicode):
                        lbl.grid(row=0, column=0)
                        b.grid(row=0, column=1)
                        t = Tkinter.Text(t_frame, width=(6 + 1) * 5,
                                         height=data.count(" ") // 5 + 2)
                        t.insert(Tkinter.END, data)
                        t.grid(row=1, column=0)
                        datatables_s.append([k, caption, t, selected])
                    else:
                        rows = data
                        lbl.grid(row=0, column=0, columnspan=len(rows[0]))
                        b.grid(row=0, column=len(rows[0]))
                        r = 1
                        for row in rows:
                            c = 0
                            for line in row:
                                lbl = Tkinter.Label(t_frame, text=line)
                                lbl.grid(row=r, column=c)
                                c += 1
                            r += 1
                            if r == 50:
                                msg = "Table is too large, "\
                                      "omitting remaining rows."
                                lbl = Tkinter.Label(t_frame, text=msg)
                                lbl.grid(row=r, columnspan=c)
                                break
                        datatables_s.append([k, caption, rows, selected])
                    t_frame.grid(row=num, column=0)
                    # TO DO: checkbuttons for "(un)select all".
                    num += 1

                scrlbr = Tkinter.Scrollbar(window, command=cnvs.yview)
                scrlbr.grid(row=0, column=2, rowspan=2, sticky='ns')
                cnvs.configure(yscrollcommand=scrlbr.set)
                frame.update_idletasks()
                rgn = (0, 0, frame.winfo_width(), frame.winfo_height())
                cnvs.configure(width=frame.winfo_width(), scrollregion=rgn)

                b = Tkinter.Button(window, text="Done",
                                   command=lambda window=window, paper=paper,
                                   value=datatables_s:
                                   self.update_paper_parameter(window, paper,
                                                               "datatables",
                                                               value))
                b.grid(row=1, column=0)
                b = Tkinter.Button(window, text="Cancel",
                                   command=lambda window=window,
                                   paper=paper: self.show_paper_info(window,
                                                                     paper))
                b.grid(row=1, column=1)
            else:
                datatables = {}
                self.update_paper_parameter(window, paper, "datatables",
                                            datatables)
        else:
            if not paper.datatables:
                lbl = Tkinter.Label(window, text="No datatables found",
                                    font=HEADING_FONT)
                lbl.grid(row=0)
                r = 1
            else:
                cnvs = Tkinter.Canvas(window, width=1200, height=800)
                cnvs.grid(row=0, column=0, columnspan=2)

                frame = Tkinter.Frame(cnvs)
                cnvs.create_window(0, 0, window=frame, anchor='nw')

                num = 0
                keys = paper.datatables.keys()
                keys.sort()
                for k in keys:
                    (caption, data) = paper.datatables[k]
                    t_frame = Tkinter.Frame(frame)
                    lbl = Tkinter.Label(t_frame, text=caption,
                                        font=HEADING_FONT)
                    if isinstance(data, str) or isinstance(data, unicode):
                        lbl.grid(row=0, column=0)
                        lbl = Tkinter.Label(t_frame, text=data, wraplength=600)
                        lbl.grid(row=1, column=0)
                    else:
                        rows = data
                        lbl.grid(row=0, column=0, columnspan=len(rows[0]))
                        r = 1
                        for row in rows:
                            c = 0
                            for line in row:
                                lbl = Tkinter.Label(t_frame, text=line)
                                lbl.grid(row=r, column=c)
                                c += 1
                            r += 1
                            if r == 50:
                                msg = "Table is too large, "\
                                      "omitting remaining rows."
                                lbl = Tkinter.Label(t_frame, text=msg)
                                lbl.grid(row=r, columnspan=c)
                                break
                    t_frame.grid(row=num, column=0)
                    num += 1

                scrlbr = Tkinter.Scrollbar(window, command=cnvs.yview)
                scrlbr.grid(row=0, column=2, rowspan=2, sticky='ns')
                cnvs.configure(yscrollcommand=scrlbr.set)
                frame.update_idletasks()
                rgn = (0, 0, frame.winfo_width(), frame.winfo_height())
                cnvs.configure(width=frame.winfo_width(), scrollregion=rgn)
            b = Tkinter.Button(window, text="Back",
                               command=lambda window=window, paper=paper:
                               self.show_paper_info(window, paper))
            b.grid(row=1)

    def export_paper_text(self, paper):
        """ Export full text of a paper into a file. """
        with asksaveasfile() as f:
            if f:
                text = paper.get_text()
                f.write(text)

    def export_texts(self):
        """ Export texts of all papers into a directory.

        Each text is saved into a file named "papername.txt".
        """
        d = askdirectory()
        for p in self.papers:
            text = p.get_text()
            with open(path_join(d, "%s.txt" % p.fname), "w") as f:
                f.write(text)

    def export_all(self, quick=False, n=None, n_p=None, errors=None, attr=None,
                   csv=None):
        """ Export all papers' metadata.

        Quick export: determine metadata first if none is available,
        skipping all user interaction.
        """
        if not self.papers:
            tkMessageBox.showwarning("Nothing to export",
                                     "No papers to export.")
        elif n is None:
            if self.unsaved_papers():
                msg = "Some papers were changed. "\
                      "These changes will be saved before "\
                      "export will be performed. Proceed?"
                if tkMessageBox.askyesno("Export", msg):
                    self.save_paper()
                else:
                    return 0
            self.window.update_idletasks()
            n = 1
            n_p = 0
            errors = {}
            s = "document name,mc datasets,real datasets,other datasets,"\
                "dataset tables"
            for a in Paper.attributes_general:
                s += ",%s" % a
            csv = [s + "\n"]
            attr = {}
            attr["mc_datasets"] = []
            attr["real_datasets"] = []
            attr["other_datasets"] = []
            attr["dataset_tables"] = []
            for a in Paper.attributes_general:
                attr[a] = []
            self.window.after(100, lambda: self.export_all(quick, n, n_p,
                                                           errors, attr, csv))
        else:
            p = self.papers[n - 1]
            msg = "Performing export %d/%d. Paper: %s. Please, wait..."\
                  % (n, len(self.papers), p.fname)
            self.status_set(msg)
            self.window.update_idletasks()
            try:
                outp = p.export(quick)
                if outp:
                    n_p += 1
                    s = "%s," % p.fname
                    if "mc_datasets" in outp["content"]:
                        attr["mc_datasets"].append(p.fname)
                        s += "1,"
                    else:
                        s += ","
                    if "real_datasets" in outp["content"]:
                        attr["real_datasets"].append(p.fname)
                        s += "1,"
                    else:
                        s += ","
                    other = ""
                    tables = []
                    for c in outp["content"]:
                        if not other and c.endswith("datasets") and\
                           not c.startswith("mc") and not c.startswith("real"):
                            attr["other_datasets"].append(p.fname)
                            other = "1"
                        if c.startswith("table"):
                            tables.append(c[len("table_"):])
                    tables.sort()
                    tables = " ".join(tables)
                    if tables:
                        attr["dataset_tables"].append(p.fname)
                    s += "%s,%s," % (other, tables)
                    for a in Paper.attributes_general:
                        if outp["content"]["plain_text"][a]:
                            attr[a].append(p.fname)
                            add = outp["content"]["plain_text"][a]
                            if isinstance(add, list):
                                add = " ".join(add)
                            s += "%s," % add
                        else:
                            s += ","
                    csv += s.rstrip(",") + "\n"
            except Exception as e:
                outp = False
                errors[p.fname] = e
            if n < len(self.papers):
                n += 1
                self.window.after(100, lambda: self.export_all(quick, n, n_p,
                                                               errors, attr,
                                                               csv))
            else:
                msg = False
                if errors:
                    keys = errors.keys()
                    keys.sort(cmp=cmp_papernames)
                    msg = "These papers were not exported for some reason:\n\n"
                    for e in keys:
                        msg += "%s : %s\n\n" % (e, errors[e])
                    with open(ERRORS_FILE, "w") as f:
                        f.write(msg)
                with open(STAT_FILE, "w") as f:
                    csv += "TOTAL\n"
                    if n_p:
                        m = 100.0 / n_p
                    else:
                        m = 100.0
                    s = "%d,%d,%d,%d,%d," % (n_p,
                                             len(attr["mc_datasets"]),
                                             len(attr["real_datasets"]),
                                             len(attr["other_datasets"]),
                                             len(attr["dataset_tables"]))
                    s_p = "100%%,%f%%,%f%%,%f%%,%f%%,"\
                          % (float(len(attr["mc_datasets"])) * m,
                             float(len(attr["real_datasets"])) * m,
                             float(len(attr["other_datasets"])) * m,
                             float(len(attr["dataset_tables"])) * m)
                    for a in Paper.attributes_general:
                        s += "%d," % len(attr[a])
                        s_p += "%f%%," % (float(len(attr[a])) * m)
                    csv += s.rstrip(",") + "\n"
                    csv += s_p.rstrip(",") + "\n"
                    f.writelines(csv)
                self.status_set("")
                if msg:
                    scrollable_warning(self.window, msg,
                                       "Unable to export papers")


if __name__ == "__main__":
    root = Tkinter.Tk()
    mngr = Manager(root)
