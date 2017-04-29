# -*- coding: utf-8 -*-
import json, os, re, shutil, sys
from shutil import rmtree

from pdfwork import *
from xmltable import *

CONFIG_FILE = "config.json"
default_cfg = {
        "WORK_DIR":             os.getcwd(),
        "DETERMINE_TITLE":      False,
        "OPEN_INTERVALS_TEXT":  False,
        "OPEN_INTERVALS_TABLES":False,
        "HDFS_PDF_DIR":         "",
        "HDFS_DOWNLOAD_COMMAND":"hadoop fs -get"
    }

def load_config(default_cfg):
    save_needed = False
    try:
        with open(CONFIG_FILE, "r") as f:
            loaded_cfg = json.load(f)
    except Exception as e:
        sys.stderr.write("Exception while loading config: %s\n" % e)
        sys.stderr.write("No config file loaded, using default values\n")
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
        json.dump(cfg, f, indent = 4)

cfg, save_needed = load_config(default_cfg)
if save_needed:
    save_config(cfg)

def path_join(a, b):
    # Wrapper around os.path.join to account for possible different separators in paths.
    return os.path.join(a, b).replace("\\", "/")

if __name__ == "__main__":
    try:
        from Tkinter import *
        from tkFileDialog import askdirectory, askopenfilename, askopenfilenames, asksaveasfile
        import tkMessageBox
    except Exception as e:
        sys.stderr.write("Exception while loading Tkinter: %s\n" % e)
        sys.stderr.write("Tkinter and/or stuff related to it cannot be loaded, graphical interface will not work\n")

    PAPERS_DIR = path_join(cfg["WORK_DIR"], "papers") # Directory for papers' directories.
    EXPORT_DIR = path_join(cfg["WORK_DIR"], "export") # Directory for exported files.
    NO_ATTRS_FILE = path_join(EXPORT_DIR, "stat.txt") # File for information about exported papers with missing attributes.
    HEADING_FONT = ("Times New Roman", 20) # Font used for headings in the program.

TXT_DIR = "txt" # Name of the subdirectory with txt files in a paper's directory.
XML_DIR = "xml" # Name of the subdirectory with xml files in a paper's directory.
METADATA_FILE = "metadata.json" # Name of the file which holds the metadata extracted from a paper.


# TO DO
# Luminosity = x+-y fb-1
# Categories editor
# Parallel extraction
# Manager hanging when extracting. Partially fixed - now status bar is updated correctly, manager still hangs otherwise but it's questionable if much can be done here.
# Good run lists
# Finding text blocks on each page, determining their type and processing them accordingly. This will help in solving problems like:
## Table headers above table. PDF 2015-170 may be a good place to test this. Table 2 (which is not needed) has a figure above it, which must be accounted for.
## Tables on several pages
## Lines construction regarding Y-axis (see PDF 609)
## Campaigns (and probably other things?) should NOT be searched in References.
# Use list(list) where appropriate.
# Additional independent module which will check json files and try to filter out wrong datasets and tables, or fix them.
# * and % in dataset names.
# "-" in dataset names instead of "_".
# Rewrite xmltable.py
# Verification
# Window resizing and scrollbar activation

class DatasetCategory:
    def __init__(self, name, string):
        self.name = name
        self.reg = re.compile(string, re.X) # Standard regular expression for finding datasets.
        self.reg_spaces = re.compile(string.replace("_", "\ ").replace("\w", "a-zA-Z0-9 "), re.X) # Same as self.reg, but with spaces instead of underscores. This is required because pdfminer sometimes reads underscores as spaces, especially in a document with small font size.
##        self.reg_dashes = re.compile(string.replace("_", "\-").replace("\w", "a-zA-Z0-9-"), re.X) # This does not works, probably because "-" is a special character, and should be "-" or "\-" in different places of regular expressions. | Same as self.reg, but with dashes instead of underscores. This is required because at least one document lists datasets in such way.
    def find(self, text, intervals, datasets):
#        print "INTERVALS", intervals
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
    #                print "STRING WITH INTERVALS:", s
                    nums = re.findall("INTERVAL(\d+)!", s)
                    arr = []
                    for n in nums:
                        arr.append(len(intervals[int(n)]))
                    size = min(arr) # TO DO: If some intervals are shorter then it should be raised as a warning somewhere...
                    for i in range(0, size):
                        ns = s
                        for n in nums:
    #                        print intervals[int(n)]
                            ns = re.sub("INTERVAL" + n + "!", intervals[int(n)][i], ns)
    #                    print "NEW_STRING:", ns
                        if self.reg.match(ns):
                            datasets[self.name].append([ns, False])
                        elif self.reg_spaces.match(ns):
                            datasets[self.name].append([ns.replace(" ", "_"), "spaces"])
    ##                    elif self.reg_dashes.match(ns):
    ##                        datasets[self.name].append([ns.replace("-", "_"), "dashes"])
                else:
                    res = 0
                    if self.reg.match(s):
                        res = 1
                    elif self.reg_spaces.match(s):
                        res = 2
                    for i in range(0, len(intervals)):
                        s = re.sub("INTERVAL%d!" % (i), intervals[i], s)
                    if res == 1:
                        datasets[self.name].append([s, False])
                    elif res == 2:
                        datasets[self.name].append([s.replace(" ", "_"), "spaces"])
            else:
                if self.reg.match(s):
                    datasets[self.name].append([s, False])
                elif self.reg_spaces.match(s):
                    datasets[self.name].append([s.replace(" ", "_"), "spaces"])
##                elif self.reg_dashes.match(s):
##                    datasets[self.name].append([s.replace("-", "_"), "dashes"])
        return (text, datasets)

# This does not works, probably because of differences between "" and r"""""" (re module throws exception).
##f = open(CATEGORIES_FILE, "r")
##categories = json.load(f)
##f.close()
##dataset_categories = []
##for c in categories:
##    regular = ""
##    for [r, comment] in c["regular"]:
##        regular += r
##    dataset_categories.append(DatasetCategory(c["name"], regular))

group = DatasetCategory("group", r"""group                      # Indicates group dataset.
                                           \n*\.\n*             # Field separator
                                           [a-zA-Z\d\-:]+       # Group name. Examples: phys-higgs, phys-beauty.
                                           (\n*[._]\n*[a-zA-Z\d\-:!]+)+
                                           """)    
user = DatasetCategory("user", r"""user                         # Indicates user dataset.
                                           \n*\.\n*             # Field separator
                                           [a-zA-Z\d\-:]+       # User name.
                                           (\n*[._]\n*[a-zA-Z\d\-:!]+)+
                                           """)    
montecarlo = DatasetCategory("montecarlo", r"""mc\d\d           # Project. Examples: mc08, mc12.
                                           \n*_\n*              # Field part separator
                                           [a-zA-Z\d!]+         # Project sub tag. Examples: 7TeV, 1beam, cos.
                                           \n*\.\n*             # Field separator
                                           [\dINTERVAL!]+       # DataSet ID(DSID)
                                           (\n*[._]\n*[a-zA-Z\d\-:!]+)+
                                           (\n*_\n*[a-z]\d+)+   # AMITag or several
                                           (_tid\d+(_\d\d)?)?   # Possible production system task and subtask numbers                                           
                                           """)    
physcont = DatasetCategory("physcont", r"""[a-zA-Z\d\-_\.:!]+
                                           \n*\.\n*             # Field separator
                                           PhysCont             # prodStep - it's always the same.
                                           \n*\.\n*             # Field separator
                                           [a-zA-Z\d\-_\.:!]+   #
                                           \n*\.\n*             # Field separator
                                           [t0proge]+\d\d_v?\d\d# version.
                                           (\n*_\n*[a-z]\d+)*   # Possible AMITag or several.
                                           """)
calibration = DatasetCategory("calibration", r"""data\d\d_calib # Project tag. Example: data08_calib.
                                           \n*\.\n*             # Field separator
                                           [\dINTERVAL!]+       # runNumber (8 digits) or timestamp (10 digits)
                                           \n*\.\n*             # Field separator
                                           [a-zA-Z\d\-_\.:!]+   #
                                           \n*\.\n*             # Field separator
                                           RAW                  #
                                           """)
realdata = DatasetCategory("realdata", r"""data\d\d             # Project tag. Examples: data09, data10.
                                           \n*_\n*              # Field part separator
                                           [a-zA-Z\d!]+         # Project sub tag. Examples: 7TeV, 1beam, cos.
                                           \n*\.\n*             # Field separator
                                           [\dINTERVAL!]+       # runNumber (8 digits) or timestamp (10 digits)
                                           \n*\.\n*             # Field separator
                                           [a-zA-Z\d\-_\.:!]+   #
                                           """)
database = DatasetCategory("database", r"""ddo                  # Project tag.
                                           \n*\.\n*             # Field separator
                                           [\dINTERVAL!]+       # 
                                           \n*\.\n*             # Field separator
                                           [a-zA-Z\d\-_\.:!]+   #
                                           """)

category_export_dict = {
        "group":"group",
        "user":"user",
        "montecarlo":"mc",
        "physcont":"cont",
        "calibration":"calib",
        "realdata":"real",
        "database":"db"
    }

# Regular expressions
#dataset_categories = [group, user, montecarlo, physcont, calibration, realdata, database]
dataset_categories = [montecarlo, physcont, calibration, realdata, database] # We don't need group and user datasets for now.
re_pdfname = re.compile("/([^./]+)\.pdf$") # Path must have / as separator, not \.
re_table_header = re.compile("Table \d+:.*?\n\n", re.DOTALL)
re_table_header_short = re.compile("Table (\d+):")
re_table_datasets = re.compile("(?:sample|dataset|run)")
re_column_with_datasets = re.compile("^(?:ds[-_ ]?|mc[-_ ]?|data ?|dataset ?|period|request ?|run ?|sample ?)(?:id|number|period|range|sample|set)")
re_dsid = re.compile("^\d{4,8}$")
re_dsid_diap = re.compile("^\d{4,8}-\d{1,8}$")
re_xml_symbol = re.compile("^<text[^>]+ size=\"([0-9.]+)\">(.+)</text>$")
re_xml_empty_symbol = re.compile("^<text> </text>$")
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
                                )"""
                         , re.X)
re_energy = re.compile("(\d+\.?\d*) (G|T)eV")
re_luminosity = re.compile("(\d+\.?\d*) ?(m|n|p|f)b(?:−|\(cid:0\))1") # WARNING: this "fb-1" is in UTF-8 coding and was copied from miner output. Simple "fb-1" does not works.
re_collisions = re.compile("(proton-proton|heavy-ion|pp) collisions")
re_year = re.compile("(?:acquired|collected|measured|recorded).{0,100}(20\d\d)", re.DOTALL)
re_year_general = re.compile(".{0,100} 20\d\d.{0,100}")
re_interval = re.compile("\[(?:[0-9][\\/][0-9\\/\n]+|[0-9]+-[0-9]+)\]") # interval must contain at least two numbers, i.e. [1/2] or [3\4\5].
re_link = re.compile("(.*)\n? ?(https?://cds\.cern\.ch/record/\d+)")

def find_cut_reg(reg, text):
    # Find patterns matching regular expression in text and cut them out.
    results = []
    f = True
    while f:
        f = reg.search(text)
        if f:
            text = text.replace(f.group(0), "")
            results.append(f.group(0).replace("\n", ""))
    return (results, text)

def mask_intervals(text):
    # Cuts out all bracketed intervals [...] from the text which can be present in the datasets names and replaces them with INTERVALnumber! strings.
    intervals = []
    m = True
    i = 0
    while m:
        m = re_interval.search(text)
        if m:
            text = text.replace(m.group(0), "INTERVAL%d!" % i)
            intervals.append(m.group(0))
            i += 1
#            print "Interval %d:%s"%(i, m.group(0).strip())
    return text, intervals

def organize_intervals(intervals):
    # Organizes the datasets. Currently changes "[1/2/3]" string into array ['1', '2', '3'] and "[9-12]" into ['09', '10', '11', '12'].
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
                    maxlen = len(max(ni1, key = lambda num: len(num)))
                    if len(min(ni1, key = lambda num: len(num))) != maxlen: # TO DO: improve this.
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
    # Transform a diapason string "X-Y" into a list [X, X+1, X+2, ..., Y-1, Y], or empty list if X > Y. Intended for table processing.
    values = []
    (s, e) = d.split("-")
    if len(e) <= len(s):
        e = s[:-len(e)] + e
        if s <= e:
            for i in range(int(s), int(e) + 1):
                values.append(str(i))
    return values

def check_all_button(v, l):
    # Command for handling Tkinter global checkbuttons which should (un)check all the checkbuttons in the list. v is a VarInt variable associated with the global checkbutton. l is a list of VarInt variables associated with checkbuttons in the list.
    s = 0
    for i in l:
        s += i.get()
    if s == len(l):
        v.set(0)
        for i in l:
            i.set(0)
    else:
        v.set(1)
        for i in l:
            i.set(1)

def cmp_papernames(x, y):
    # Compare paper names. Default cmp function thinks that, for example, "9" > "10" (it compares "9" and "1" first, and "9" > "1").
    if x.isdigit() and y.isdigit():
        return int(x) - int(y)
    else:
        return cmp(x, y)

class Paper:
    # Represents a document which needs to be analyzed, as well as files and other things associated with it.
    attributes_general = ["campaigns", "energy", "luminosity", "collisions", "data taking year", "project_montecarlo", "project_realdata", "links"]
    attributes_to_determine = attributes_general + ["title", "datasets", "datatables"] # Paper attributes which are needed but cannot be determined precisely yet (unlike, for example, number of pages).
    attributes_metadata = attributes_to_determine + ["num_pages", "rotated_pages"] # Attributes which are saved / loaded to / from a file.
    def __init__(self, fname, dirname = False):
        self.fname = fname
        if not dirname:
            self.dir = path_join(PAPERS_DIR, self.fname)
        else:
            self.dir = dirname
        self.pdf = path_join(self.dir, "%s.pdf" % (self.fname))
        self.txt_dir = path_join(self.dir, TXT_DIR)
        self.xml_dir = path_join(self.dir, XML_DIR)
        self.metadata_file = path_join(self.dir, METADATA_FILE)
        for a in self.attributes_to_determine: 
            self.__dict__[a] = None # This indicates that attributes should be determined when need to display them arises for the first time. If nothing was found, their values would be set to False or [] or {}. 

        self.num_pages = None # Number of pages in a paper.
        self.rotated_pages = None # Numbers of pages which are rotated.

        self.changed = False # This flag is set to True when part of metadata is changed, but not yet saved to the metadata file.
    def get_txt_page(self, number, text = False):
        # Fetch txt page of the paper. Result is either text(if text variable is True) or lines (if text variable is False).
        fname = path_join(self.txt_dir, "%d.txt" % (number))
        with open(fname, "r") as f:
            if text:
                r = f.read()
            else:
                r = f.readlines()
        return r
    def get_xml_page(self, number, text = False):
        # Fetch xml page of the paper. Extract xml page from PDF if not done yet. Result is either text(if text variable is True) or lines (if text variable is False).
        fname = path_join(self.xml_dir, "%d.xml" % (number))
        if not os.access(fname, os.F_OK):
            [num_pages, rotated_pages] = mine_text(self.pdf, [number], "xml", self.rotated_pages, self.xml_dir)
        with open(fname, "r") as f:
            if text:
                r = f.read()
            else:
                r = f.readlines()
        return r
    def mine_text(self):
        # Extract text from the PDF file.
        if not os.access(self.txt_dir, os.F_OK):
            os.mkdir(self.txt_dir)
        [num_pages, self.rotated_pages] = mine_text(self.pdf, folder = self.txt_dir)
        self.num_pages = num_pages
        if not os.access(self.xml_dir, os.F_OK):
            os.mkdir(self.xml_dir)
        self.get_xml_page(1)
        self.save_metadata()
    def get_text(self):
        # Read and return mined text of the document.
        text = ""
        for i in range(1, self.num_pages + 1):
            with open(path_join(self.txt_dir, "%d.txt") % (i), "r") as f:
                text += f.read()
        return text
    def clear_metadata(self):
        # Clear all non-precise document metadata and set them to None to be determined again.
        for a in self.attributes_to_determine:
            if self.__dict__[a] is not None:
                self.changed = True
                self.__dict__[a] = None
    def save_metadata(self):
        # Export metadata to a file.
        outp = {}
        for key in self.attributes_metadata:
            outp[key] = self.__dict__[key]
        with open(self.metadata_file, "w") as f:
            json.dump(outp, f, indent = 4)
        self.changed = False
    def load_metadata(self):
        # Import metadata from a file.
        if not os.access(self.metadata_file, os.R_OK):
            return 0
        with open(self.metadata_file, "r") as f:
            inp = json.load(f)
        for key in self.attributes_metadata:
            if key in inp:
                self.__dict__[key] = inp[key]
    def delete(self):
        # Delete all files associated with paper.
        rmtree(self.dir)
##    def find_title(self): # New title determining method, does not works ideally yet. Titles consisting of several lines are problematic to determine.
##        lines = self.get_xml_page(1)
##
##        d = {}
##        for l in lines:
##            m = re_xml_symbol.match(l)
##            if m:
##                size = float(m.group(1))
##                text = m.group(2)
##                if size in d.keys():
##                    d[size] += text
##                else:
##                    d[size] = text
##            elif re_xml_empty_symbol.match(l):
##                d[size] += " "
##        xml_title = False
##        print d
##        while True:
##            size = max(d.keys())
##            valid = True
##            try:
##                d[size].decode()
##            except:
##                valid = False
##            if not "atlas note" in d[size].lower() and valid:
##                xml_title = d[size]
##                break
##            else:
##                del d[size]
##                
##        print xml_title
##        if not xml_title:
##            return False
##        
##        lines = self.get_txt_page(1)
##        title = ""
##        for l in lines:
##            if len(l) <= 4 or l.startswith("Supporting Note") or l.startswith("ATLAS NOTE"):
##                continue
##            words = l.split()
##            i = 0
##            for w in words:
##                try:
##                    w_in = w in xml_title # This throws exception sometimes, something about ascii codec unable to decode.
##                except:
##                    w_in = False
##                if len(w) > 1 and w_in:
##                    i += 1
##            if i > 1 or (len(words) == 1 and i == 1):
##                title += l.replace("\n", " ")
##            elif title:
##                break
##        return title
    def find_attributes_general(self):
        # Find general attributes in a document.
        attrs = {}
#        attrs["title"] = self.find_title()
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
            attrs["luminosity"] = tmp.group(0).replace("−","-").replace("(cid:0)", "-")

        links = re_link.findall(pages)
        attrs["links"] = {}
        for (key, value) in links:
            attrs["links"][key] = value

        pages = pages.lower()

        attrs["collisions"] = False
        tmp = re_collisions.search(pages)
        if tmp:
            attrs["collisions"] = tmp.group(1)
            if attrs["collisions"] == "pp":
                attrs["collisions"] = "proton-proton"

        attrs["data taking year"] = False
        tmp = re_year.search(text)
        if tmp:
            attrs["data taking year"] = tmp.group(1)
        else:
            attrs["possible years"] = list(set(re_year_general.findall(text)))

        if attrs["campaigns"] and attrs["energy"]:
            mcc = False
            for c in attrs["campaigns"]:
                if c.startswith("mc"):
                    mcc = c
                    break
            if mcc:
                attrs["project_montecarlo"] = mcc + "_" + attrs["energy"].replace(" ", "")
            else:
                attrs["project_montecarlo"] = False
        else:
            attrs["project_montecarlo"] = False

        if attrs["data taking year"] and attrs["energy"]:
            attrs["project_realdata"] = "data" + attrs["data taking year"][2:4] + "_" + attrs["energy"].replace(" ", "")
        else:
            attrs["project_realdata"] = False

        return attrs
    def find_datasets(self):
        # Find datasets in text of the document.
        text = self.get_text()
        text, intervals = mask_intervals(text)
        if cfg["OPEN_INTERVALS_TEXT"]:
            intervals = organize_intervals(intervals)
        datasets = {}

        for c in dataset_categories:
            (text, datasets) = c.find(text, intervals, datasets)
        return (text, datasets)
    def find_datatables(self):
        # Find tables in the document which may contain datasets.
        pages_with_tables = []
        headers_data = {}
        n = 1
        while n <= self.num_pages: # Find pages containing table headers.
            text = self.get_txt_page(n, True)
#            print n, re_table_header.findall(text.lower())
            page_headers = re_table_header.findall(text)
            page_headers_data = {}
            for h in page_headers: # Among the headers find ones which may hint that their tables contain datasets. Store these headers, their numbers and their pages.
                if re_table_datasets.search(h.lower()):
                    num = int(re_table_header_short.match(h).group(1))
                    page_headers_data[num] = h
            if page_headers_data:
                pages_with_tables.append(n)
                headers_data.update(page_headers_data)
            n += 1

#        print "PAGES WITH DATASETS TABLES", pages_with_tables

        datatables = {}
        for n in pages_with_tables: # Extract all tables from selected pages.
            text = self.get_xml_page(n, True)
            tables = get_tables_from_text(text)
            for table in tables: # Save headers and tables matching selected numbers and having dataset-related columns.
                num = int(re_table_header_short.match(table.header).group(1))
                if num in headers_data:
#                    print "TABLE WITH HEADER", headers_data[num].strip(), "MAY CONTAIN DATASETS"
                    data_column = -1
                    skip_first = False
                    for rnum in range(0, min(2, len(table.rows))): # Check first two rows. Sometimes there is an additional row above main row.
                        for i in range(0, len(table.rows[rnum])):
                            if re_column_with_datasets.match(table.rows[rnum][i].text.lower()):
#                                print "COLUMN", table.rows[rnum][i].text.lower(), "IN TABLE", num, "HINTS THAT IT CONTAINS DATASETS"
                                data_column = i
                                if rnum == 1: # This means that first row contains some kind of header, or rubbish, or something else, and columns are defined in the second one. First one must be skipped in such case.
#                                    print "SKIPPING FIRST ROW"
                                    skip_first = True
                                break
                        if data_column >= 0:
                            break
                    if data_column >= 0: # Here: insert check that dataset column contains mostly \d\d\d\d\d\d. Also: duplicate rows in case of diapasones.
                        rows = []
                        rows_with_proper_id = 1 # Start at 1 instead of 0 because the first row (which defines columns) will not contain a proper dataset/run id.
                        diaps = False
                        for row in table.rows:
                            if skip_first:
                                skip_first = False
                                continue
                            row = [line.text.strip() for line in row]
#                            print row[data_column]
                            if re_dsid.match(row[data_column]):
                                rows_with_proper_id += 1
                            elif re_dsid_diap.match(row[data_column]):
                                rows_with_proper_id += 1
                                diaps = True
                            rows.append(row)
                        coef = float(rows_with_proper_id) / len(rows)
#                        print rows_with_proper_id, "OUT OF", len(rows), "ROWS HAVE PROPER DATASET ID. COEFFICIENT:", coef
                        if coef >= 0.7 and coef <= 1:
                            if cfg["OPEN_INTERVALS_TABLES"] and diaps:
#                                print "TABLE CONTAINS DATASET DIAPASONS, PROCESSING THEM AND MULTIPLYING ROWS"
                                rows_new = []
                                for row in rows:
                                    if re_dsid_diap.match(row[data_column]):
                                        values = process_diapason(row[data_column])
                                        for v in values:
                                            row_new = list(row)
                                            row_new[data_column] = v
                                            rows_new.append(row_new)
                                    else:
                                        rows_new.append(row)
                                    rows = rows_new
                            datatables[num] = (headers_data[num], rows)
#                        elif coef < 0.7:
#                            print "COEFFICIENT IS LOWER THAN 0.7. SKIPPING TABLE", num

        return datatables
    def export(self, quick = False, outf = False):
        # Export metadata into file in export directory. Quick export: if a part of metadata was never determined, the corresponding procedure would be used with all user interaction skipped.

##        print self.fname
##        paper_date = re.search("((?:january|february|march|april|may|june|july|august|september|october|november|december).*20\d\d)", self.get_txt_page(1, True).lower())
##        if paper_date:
##             d = paper_date.group(1)
##             print "date:", d
##
##        text = self.get_text()
##        m = re_year.findall(text)
##        if m:
##            print m
####            for t in m:
####                if d not in t.lower():
####                    print t
##        else:
##            print "None"
##        print "\n"
##        return True


        outp = {}
        if not outf:
            outf = path_join(EXPORT_DIR, "%s.json" % (self.fname))
            
        outp["fname"] = self.fname # Some applications processing exported data may discard the filename but it must be preserved.

        if self.title is not None:
            outp["title"] = self.title

        outp["content"] = {}
        outp["content"]["plain_text"] = {}
        if self.campaigns is not None: # All general attributes are determined together, so we can check only one.
            for a in self.attributes_general:
                outp["content"]["plain_text"][a] = self.__dict__[a]
        elif quick:
            attrs = self.find_attributes_general()
            for a in attrs:
                outp["content"]["plain_text"][a] = attrs[a]

        if self.datasets is not None:
            for category in self.datasets:
                outp["content"][category_export_dict[category] + "_datasets"] = self.datasets[category]
        elif quick:
            (text, datasets) = self.find_datasets()
            for category in datasets:
                d = []
                for [name, special] in datasets[category]:
                    d.append(name)
                outp["content"][category_export_dict[category] + "_datasets"] = d
        if self.datatables is not None:
            for num in self.datatables:
                outp["content"]["table_" + str(num)] = self.datatables[num]
        elif quick:
            tables = self.find_datatables()
            for num in tables:
                outp["content"]["table_" + str(num)] = tables[num]

        if outp:
            with open(outf, "w") as f:
                json.dump(outp, f, indent = 4)
        return outp

class Manager:
    # Main class of the application, performs most of the work.
    def __init__(self, window):
        self.window = window
        self.window.title("Support notes manager")
        main_menu = Menu(self.window)
        papers_menu = Menu(main_menu, tearoff = 0)
        export_menu = Menu(main_menu, tearoff = 0)
        papers_menu.add_command(label = "Add...", command = self.add_papers)
        papers_menu.add_command(label = "Save all", command = self.save_paper)
        papers_menu.add_command(label = "Clear all", command = self.clear_paper)
        papers_menu.add_command(label = "Exit", command = self.finish)
        main_menu.add_cascade(label = "Papers", menu = papers_menu)
        export_menu.add_command(label = "Quick export", command = lambda: self.export_all(quick = True))
        export_menu.add_command(label = "Export", command = self.export_all)
        export_menu.add_command(label = "Export texts", command = self.export_texts)
        main_menu.add_cascade(label = "Export", menu = export_menu)
        main_menu.add_command(label = "Preferences", command = self.preferences)
        self.window.config(menu = main_menu)

        self.papers = []
        if not os.access(PAPERS_DIR, os.F_OK):
            os.mkdir(PAPERS_DIR)
        objs = os.listdir(PAPERS_DIR) # Check papers directory and load papers from it.
        for o in objs:
            if os.path.isdir(path_join(PAPERS_DIR, o)):
                p = Paper(o)
                p.load_metadata()
                self.papers.append(p)

        if not os.access(EXPORT_DIR, os.F_OK):
            os.mkdir(EXPORT_DIR)

        self.cnvs = Canvas(self.window, width = 1200, height = 800)
        self.cnvs.grid(row = 1, column = 0)
        self.frame = Frame(self.cnvs)
        self.cnvs.create_window(0, 0, window = self.frame, anchor = 'nw')
        scrlbr = Scrollbar(self.window, command = self.cnvs.yview)
        scrlbr.grid(row = 0, rowspan = 2, column = 1, sticky = 'ns')
        self.cnvs.configure(yscrollcommand = scrlbr.set)

        self.status = Label(self.window, text = "", bd = 1, relief = SUNKEN)
        self.status.grid(row = 2,  sticky = 'we')

        self.window.protocol("WM_DELETE_WINDOW", self.finish) # Intercept closing the program via Alt + F4 or other methods to perform a clean exit.

        self.redraw()
        self.window.mainloop()
    def unsaved_papers(self):
        # Returns True if at least one paper was changed but not yet saved.
        for p in self.papers:
            if p.changed:
                return True
        return False
    def finish(self):
        # Exit application. Ask about saving the changes first if any are present.
        if self.unsaved_papers():
            if tkMessageBox.askyesno("Save changes?", "Some papers were changed. Do you want to save these changes?"):
                self.save_paper()
            self.window.destroy()
        else:
            self.window.destroy()
    def status_set(self, text = ""):
        # Update status bar.
        self.status.configure(text = text)
    def redraw(self):
        # Redraw the main window.
        for c in self.frame.winfo_children():
            c.destroy()

        self.papers.sort(cmp = cmp_papernames, key = lambda paper: paper.fname)
        
        r = 0
        for p in self.papers:
            if p.changed:
                t = p.fname + "*"
            else:
                t = p.fname
            b = Button(self.frame, text = t)
            b.config(command = lambda paper = p: self.show_paper_info(False, paper))
            b.grid(row = r, column = 0)
            if p.title is not None:
                l = Label(self.frame, text = p.title)
                l.grid(row = r, column = 1)
            r += 1
            
        self.frame.update_idletasks()
        self.cnvs.configure(scrollregion=(0, 0, self.frame.winfo_width(), self.frame.winfo_height()))
    def preferences(self):
        # Show preferences window.
        w = Toplevel()
        w.title("Preferences")
        w.transient(self.window)
        w.grab_set()
        determine_title = BooleanVar()
        determine_title.set(cfg["DETERMINE_TITLE"])
        open_intervals_text = BooleanVar()
        open_intervals_text.set(cfg["OPEN_INTERVALS_TEXT"])
        open_intervals_tables = BooleanVar()
        open_intervals_tables.set(cfg["OPEN_INTERVALS_TABLES"])
        work_dir = StringVar()
        work_dir.set(cfg["WORK_DIR"])

        frame = Frame(w)

        l = Label(frame, text = "Working directory")
        l.grid(row = 0, column = 0)
        e = Entry(frame, width = 100, textvariable = work_dir)
        e.grid(row = 0, column = 1)

        l = Label(frame, text = "Determine papers' titles")
        l.grid(row = 1, column = 0)
        b = Checkbutton(frame, variable = determine_title)
        b.grid(row = 1, column = 1)
        
        l = Label(frame, text = "Open intervals in text")
        l.grid(row = 2, column = 0)
        b = Checkbutton(frame, variable = open_intervals_text)
        b.grid(row = 2, column = 1)

        l = Label(frame, text = "Open intervals in tables")
        l.grid(row = 3, column = 0)
        b = Checkbutton(frame, variable = open_intervals_tables)
        b.grid(row = 3, column = 1)

        frame.grid(row = 0, column = 0)
        b = Button(w, text = "Done", command = w.destroy)
        b.grid(row = 1, column = 0)
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
        save_config(cfg)
        if restart:
            tkMessageBox.showwarning("Restart needed", "Program needs to be restarted to apply the changes.")
            self.finish()
    def add_papers(self, fnames = None, errors = None, n = None):
        # Add new papers from PDF files.
        if fnames is None:
            fnames = askopenfilenames(initialdir = cfg["WORK_DIR"], filetypes = [("PDF files", ".pdf")])
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
                    self.status_set("Extracting text %d/%d. Paper: %s. Please, wait..." % (n, len(fnames), fname))
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
                            rmtree(paper_dir)                            
            if n < len(fnames):
                n += 1
                self.window.after(100, lambda: self.add_papers(fnames, errors, n))
            else:
                self.status_set("")
                self.window.update_idletasks()
                if errors:
                    msg = "These papers were not added for some reason:\n\n"
                    for e in errors:
                        msg += "%s : %s\n\n" % (e, errors[e])
                    tkMessageBox.showwarning("Unable to add papers", msg)
                self.redraw()
    def clear_paper(self, window = False, paper = False):
        # Clear a single or all papers' metadata.
        if paper:
            paper.clear_metadata()
        else:
            if tkMessageBox.askyesno("Clear all metadata?", "Are you sure you want to clear all papers' metadata?"):
                for p in self.papers:
                    p.clear_metadata()
        if window:
            window.destroy()
        self.redraw()
    def save_paper(self, paper = False, finish = False):
        # Save a single or all papers' metadata. Exit afterwards if finish is True.
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
        # Delete paper.
        if tkMessageBox.askyesno("Delete paper?", "Are you sure you want to delete paper %s?"%(paper.fname)):
            self.papers.remove(paper)
            paper.delete()
            if window:
                window.destroy()
            self.redraw()
    def show_paper_info(self, window, paper):
        # Display information about a paper and possible actions with it.
        if paper.title is None and cfg["DETERMINE_TITLE"]:
            self.determine_paper_title_step_1(window, paper)
            return 0
        if not window:
            window = Toplevel()
        else:
            for c in window.winfo_children():
                c.destroy()
        window.title("Info: %s" % paper.fname)
        l = Label(window, text = "File name: %s" % paper.fname)
        l.grid(row = 0, column = 1)
        b = Button(window, text = "Save", command = lambda paper = paper: self.save_paper(paper))
        b.grid(row = 0, column = 2)        
        b = Button(window, text = "Export", command = lambda paper = paper: paper.export()) # Maybe this should ask for save before export...
        b.grid(row = 0, column = 3)        
        b = Button(window, text = "Clear", command = lambda window = window, paper = paper: self.clear_paper(window, paper))
        b.grid(row = 0, column = 4)        
        b = Button(window, text = "Delete", command = lambda window = window, paper = paper: self.delete_paper(window, paper))
        b.grid(row = 0, column = 5)        
        l = Label(window, text = "Title: %s" % paper.title)
        l.grid(row = 1, columnspan = 5)
        l = Label(window, text = "Pages: %d" % paper.num_pages)
        l.grid(row = 2, columnspan = 5)
        l = Label(window, text = "Rotated pages: %s" % str(paper.rotated_pages))
        l.grid(row = 3, columnspan = 5)
        b = Button(window, text = "Attributes", command = lambda window = window, paper = paper: self.show_paper_attributes(window, paper))
        b.grid(row = 4, columnspan = 5)        
        b = Button(window, text = "Datasets", command = lambda window = window, paper = paper: self.show_paper_datasets(window, paper))
        b.grid(row = 5, columnspan = 5)        
        b = Button(window, text = "Dataset tables", command = lambda window = window, paper = paper: self.show_paper_datatables(window, paper))
        b.grid(row = 6, columnspan = 5)
        b = Button(window, text = "Tables", command = lambda window = window, paper = paper: self.show_paper_page_tables(window, paper))
        b.grid(row = 7, columnspan = 5)
        b = Button(window, text = "Visualize", command = lambda window = window, paper = paper: self.show_paper_visual(window, paper))
        b.grid(row = 8, columnspan = 5)
        b = Button(window, text = "Export text", command = lambda paper = paper: self.export_paper_text(paper))
        b.grid(row = 9, columnspan = 5)
        b = Button(window, text = "Close", command = window.destroy)
        b.grid(row = 10, columnspan = 5)        
    def determine_paper_title_step_1(self, window, paper):
        # Search the first page in xml format for a possible paper titles. Ask user to pick one.
        if not window:
            window = Toplevel()
        else:
            for c in window.winfo_children():
                c.destroy()
        window.title("Determine paper title")
        lines = paper.get_xml_page(1)

        d = {}
        r = re.compile("^<text[^>]+ size=\"([0-9.]+)\">(.+)</text>$")
        empty = re.compile("^<text> </text>$")
        for l in lines:
            m = r.match(l)
            if m:
                size = m.group(1)
                text = m.group(2)
                if size in d.keys():
                    d[size] += text
                else:
                    d[size] = text
            elif empty.match(l):
                d[size] += " "
                
        l = Label(window, text = "Please, select a string which looks like a title of the paper", font = HEADING_FONT)
        l.grid(row = 0, columnspan = 2)
        r = 1
        selection = StringVar()
        for key in d:
            if len(d[key]) > 10:
                if len(d[key]) > 50:
                    bt = d[key][:50] + "..."
                else:
                    bt = d[key]
                if r == 1:
                    selection.set(d[key])
                button = Radiobutton(window, text = d[key], variable = selection, value = d[key], wraplength = 800)
                button.grid(row = r, columnspan = 2)
                r += 1
        button = Button(window, text = "Proceed", command = lambda window = window, paper = paper, ptv = selection: self.determine_paper_title_step_2(window, paper, ptv))
        button.grid(row = r, column = 0)
        button = Button(window, text = "Cancel", command = window.destroy)
        button.grid(row = r, column = 1)
    def determine_paper_title_step_2(self, window, paper, possible_title_variable):
        # Compare the title string selected by user with the first page in txt format to construct a paper title.
        for c in window.winfo_children():
            c.destroy()
        lines = paper.get_txt_page(1)

        possible_title = possible_title_variable.get()

        title = ""
        for l in lines:
            if len(l) <= 4 or l.startswith("Supporting Note") or l.startswith("ATLAS NOTE"):
                continue
            words = l.split()
            i = 0
            for w in words:
                try:
                    w_in = w in possible_title # This throws exception sometimes, something about ascii codec unable to decode.
                except:
                    w_in = False
                if len(w) > 1 and w_in:
                    i += 1
            if i > 1 or (len(words) == 1 and i == 1):
                title += l.replace("\n", " ")
            elif title:
                break
        l = Label(window, text = "Please, correct the title, if needed.", font = HEADING_FONT)
        l.grid(row = 0, columnspan = 2)
        e = Entry(window, width = 150)
        e.insert(0, title)
        e.grid(row = 1, columnspan = 2)
        button = Button(window, text = "Done", command = lambda window = window, paper = paper, value = e: self.update_paper_parameter(window, paper, "title", value))
        button.grid(row = 2, column = 0)
        button = Button(window, text = "Back", command = lambda window = window, paper = paper: self.determine_paper_title_step_1(window, paper))
        button.grid(row = 2, column = 1)
    def update_paper_parameter(self, window, paper, param, value):
        # Update a part of paper metadata and proceed to corresponding window.
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
#                        datasets[c].append([entry.get(), special])
                        datasets[c].append(entry.get()) # special is not needed. Maybe temporary.
                if not datasets[c]:
                    del datasets[c]
            if datasets:
                for c in datasets:
                    datasets[c].sort(key = lambda d: d[0])
            paper.datasets = datasets
            self.show_paper_datasets(window, paper)
        elif param == "datatables":
            paper.datatables = {}
            for [num, header, rows, selected] in value:
                if selected.get():
                    paper.datatables[num] = (header, rows)
            self.show_paper_datatables(window, paper)
        paper.changed = True
        self.redraw()
    def show_paper_attributes(self, window, paper):
        # Determine / display paper's general attributes.
        for c in window.winfo_children():
            c.destroy()
        window.title("Attributes of %s" % paper.fname)
        if paper.campaigns is None:
            attrs = paper.find_attributes_general()
            if "possible years" in attrs:
                msg = "No year was found in %s, possible years:\n\n" % (paper.fname)
                for m in attrs["possible years"]:
                    try:
                        msg += m + "\n"
                    except:
                        for c in m:
                            try:
                                msg += c
                            except:
                                msg += "?"
                        msg += "\n"
                    msg += "_______________________________\n"
                nw = Toplevel()
                nw.title("No year found")
                t = Text(nw)
                t.insert(END, msg)
                t.config(state = DISABLED)
                t.grid(row = 0, column = 0)
                b = Button(nw, text = "Close", command = nw.destroy)
                b.grid(row = 1, column = 0)
                del attrs["possible years"]
            self.update_paper_parameter(window, paper, "general", attrs)
        else:
            r = 0
            for a in paper.attributes_general:
                if a == "links":
                    if paper.__dict__[a]:
                        l = Label(window, text = "Links:")
                        l.grid(row = r, column = 0)
                        r += 1
                        for key in paper.__dict__[a]:
                            l = Label(window, text = "%s %s" % (key, paper.__dict__[a][key]))
                            l.grid(row = r, column = 0)
                            r += 1                            
                    else:
                        l = Label(window, text = "No links")
                        l.grid(row = r, column = 0)
                        r += 1
                else:
                    l = Label(window, text = "%s:%s"%(a, str(paper.__dict__[a])))
                    l.grid(row = r, column = 0)
                    r += 1
            b = Button(window, text = "Back", command = lambda window = window, paper = paper: self.show_paper_info(window, paper))
            b.grid(row = r, column = 0)            
    def show_paper_datasets(self, window, paper):
        # Determine / display paper datasets.
        for c in window.winfo_children():
            c.destroy()
        window.title("Datasets in %s" % paper.fname)
        if paper.datasets is None:
            (text, datasets) = paper.find_datasets()

            if datasets:
                cnvs = Canvas(window, width = 1200, height = 800)
                cnvs.grid(row = 0, column = 0, columnspan = 2)

                frame = Frame(cnvs)
                cnvs.create_window(0, 0, window = frame, anchor = 'nw')
                
                r = 0
                dataset_entries = {}
                for c in datasets:
                    l = Label(frame, text = c, font = HEADING_FONT)
                    l.grid(row = r, column = 0, columnspan = 2)
                    category_selected = IntVar()
                    category_selected.set(1)
                    check_category_b = Checkbutton(frame, var = category_selected)
                    check_category_b.grid(row = r, column = 3)
                    r += 1
                    dataset_entries[c] = []
                    selected_list = []
                    datasets[c].sort(key = lambda d: d[0])
                    for [name, special] in datasets[c]:
                        l = Label(frame, text = special)
                        l.grid(row = r, column = 0)
                        e = Entry(frame, width = 150)
                        e.insert(0, name)
                        e.grid(row = r, column = 1)
                        selected = IntVar()
                        selected.set(1)
                        b = Checkbutton(frame, var = selected) # TO DO: checkbuttons for "(un)select all". 
                        dataset_entries[c].append([e, special, selected])
                        selected_list.append(selected)
                        b.grid(row = r, column = 3, pady = 5)
                        r += 1
                    check_category_b.config(command = lambda v = category_selected, l = selected_list: check_all_button(v, l)) # This command is not called when individual checkbuttons are clicked - Therefore, global checkbox will not change its look.

                scrlbr = Scrollbar(window, command = cnvs.yview)
                scrlbr.grid(row = 0, column = 2, rowspan = 2, sticky = 'ns')
                cnvs.configure(yscrollcommand = scrlbr.set)
                frame.update_idletasks()
                cnvs.configure(width = frame.winfo_width(), scrollregion=(0, 0, frame.winfo_width(), frame.winfo_height()))
                
                b = Button(window, text = "Done", command = lambda window = window, paper = paper, value = dataset_entries: self.update_paper_parameter(window, paper, "datasets", value))
                b.grid(row = 1, column = 0)
                b = Button(window, text = "Cancel", command = lambda window = window, paper = paper: self.show_paper_info(window, paper))
                b.grid(row = 1, column = 1)
            else:
                datasets = {}
                self.update_paper_parameter(window, paper, "datasets", datasets)
        else:
            if not paper.datasets:
                l = Label(window, text = "No datasets found", font = HEADING_FONT)
                l.grid(row = 0)
            else:
                cnvs = Canvas(window, width = 1200, height = 800)
                cnvs.grid(row = 0, column = 0, columnspan = 2)

                frame = Frame(cnvs)
                cnvs.create_window(0, 0, window = frame, anchor = 'nw')

                r = 0
                for k in paper.datasets:                
                    l = Label(frame, text = k, font = HEADING_FONT)
                    l.grid(row = r)
                    r += 1
#                    for [d, special] in paper.datasets[k]:
                    for d in paper.datasets[k]:
                        l = Label(frame, text = d)
                        l.grid(row = r)
                        r += 1

                scrlbr = Scrollbar(window, command = cnvs.yview)
                scrlbr.grid(row = 0, column = 2, rowspan = 2, sticky = 'ns')
                cnvs.configure(yscrollcommand = scrlbr.set)
                frame.update_idletasks()
                cnvs.configure(width = frame.winfo_width(), scrollregion=(0, 0, frame.winfo_width(), frame.winfo_height()))
            b = Button(window, text = "Back", command = lambda window = window, paper = paper: self.show_paper_info(window, paper))
            b.grid(row = 1)
    def show_paper_datatables(self, window, paper):
        # Determine / display paper tables which may contain datasets.
        for c in window.winfo_children():
            c.destroy()
        window.title("Tables with datasets in %s" % paper.fname)
        if paper.datatables is None:
            datatables = paper.find_datatables()
            if datatables:

                cnvs = Canvas(window, width = 600, height = 800)
                cnvs.grid(row = 0, column = 0, columnspan = 2)

                frame = Frame(cnvs)
                cnvs.create_window(0, 0, window = frame, anchor = 'nw')

                num = 0
                keys = datatables.keys()
                keys.sort()
                datatables_s = []
                for k in keys:
                    (header, rows) = datatables[k]
                    t_frame = Frame(frame)
                    l = Label(t_frame, text = header, font = HEADING_FONT)
                    l.grid(row = 0, column = 0, columnspan = len(rows[0]))
                    r = 1
                    for row in rows:
                        c = 0
                        for line in row:
                            l = Label(t_frame, text = line)
                            l.grid(row = r, column = c)
                            c += 1
                        r += 1
                        if r == 50:
                            l = Label(t_frame, text = "Table is too large, omitting remaining rows.")
                            l.grid(row = r, columnspan = c)
                            break
                    t_frame.grid(row = num, column = 0)
                    selected = IntVar()
                    selected.set(1)
                    b = Checkbutton(t_frame, var = selected) # TO DO: checkbuttons for "(un)select all".
                    b.grid(row = 0, column = len(rows[0]))
                    datatables_s.append([k, header, rows, selected])
                    num += 1

                scrlbr = Scrollbar(window, command = cnvs.yview)
                scrlbr.grid(row = 0, column = 2, rowspan = 2, sticky = 'ns')
                cnvs.configure(yscrollcommand = scrlbr.set)
                frame.update_idletasks()
                cnvs.configure(width = frame.winfo_width(), scrollregion=(0, 0, frame.winfo_width(), frame.winfo_height()))
                
                b = Button(window, text = "Done", command = lambda window = window, paper = paper, value = datatables_s: self.update_paper_parameter(window, paper, "datatables", value))
                b.grid(row = 1, column = 0)
                b = Button(window, text = "Cancel", command = lambda window = window, paper = paper: self.show_paper_info(window, paper))
                b.grid(row = 1, column = 1)
            else:
                datatables = {}
                self.update_paper_parameter(window, paper, "datatables", datatables)
        else:
            if not paper.datatables:
                l = Label(window, text = "No datatables found", font = HEADING_FONT)
                l.grid(row = 0)
                r = 1
            else:
                cnvs = Canvas(window, width = 1200, height = 800)
                cnvs.grid(row = 0, column = 0, columnspan = 2)

                frame = Frame(cnvs)
                cnvs.create_window(0, 0, window = frame, anchor = 'nw')

                num = 0
                keys = paper.datatables.keys()
                keys.sort()
                for k in keys:
                    (header, rows) = paper.datatables[k]
                    t_frame = Frame(frame)
                    l = Label(t_frame, text = header, font = HEADING_FONT)
                    l.grid(row = 0, column = 0, columnspan = len(rows[0]))
                    r = 1
                    for row in rows:
                        c = 0
                        for line in row:
                            l = Label(t_frame, text = line)
                            l.grid(row = r, column = c)
                            c += 1
                        r += 1
                        if r == 50:
                            l = Label(t_frame, text = "Table is too large, omitting remaining rows.")
                            l.grid(row = r, columnspan = c)
                            break
                    t_frame.grid(row = num, column = 0)
                    num += 1

                scrlbr = Scrollbar(window, command = cnvs.yview)
                scrlbr.grid(row = 0, column = 2, rowspan = 2, sticky = 'ns')
                cnvs.configure(yscrollcommand = scrlbr.set)
                frame.update_idletasks()
                cnvs.configure(width = frame.winfo_width(), scrollregion=(0, 0, frame.winfo_width(), frame.winfo_height()))
            b = Button(window, text = "Back", command = lambda window = window, paper = paper: self.show_paper_info(window, paper))
            b.grid(row = 1)
    def show_paper_page_tables(self, window, paper, e = False):
        # Extract tables from a page and display them. Debug function.
        if not e:
            for c in window.winfo_children():
                c.destroy()
            window.title("Select page")
            l = Label(window, text = "Page number(1 - %d):"%(paper.num_pages))
            l.grid(row = 0, column = 0)
            e = Entry(window, width = 10)
            e.grid(row = 0, column = 1)
            e.focus_set()
            b = Button(window, text = "Proceed", command = lambda window = window, paper = paper, e = e: self.show_paper_page_tables(window, paper, e))
            b.grid(row = 1, column = 0)
            b = Button(window, text = "Cancel", command = lambda window = window, paper = paper: self.show_paper_info(window, paper))
            b.grid(row = 1, column = 1)
        else:
            number = e.get()
            if not number.isdigit():
                return 0
            else:
                number = int(number)
                if number < 1 or number > paper.num_pages:
                    return 0
                for c in window.winfo_children():
                    c.destroy()
                window.title("Tables")
                text = paper.get_xml_page(number, True)
                tables = get_tables_from_text(text)
                for table_num in range(0, len(tables)):
                    frame = Frame(window)
                    l = Label(frame, text = "Table %d" % table_num)
                    l.grid(row = 0, column = 0, columnspan = len(tables[table_num].rows[0]))
                    r = 1
                    for row in tables[table_num].rows:
                        c = 0
                        for line in row:
                            l = Label(frame, text = line.text)
                            l.grid(row = r, column = c)
                            c += 1
                        r += 1
                    frame.grid(row = table_num, column = 0)
                    table_num += 1
                if not tables:
                    l = Label(window, text = "No tables found on page %d" % (number))
                    l.grid(row = 0, column = 0)
                    table_num = 1
                b = Button(window, text = "Back", command = lambda window = window, paper = paper: self.show_paper_info(window, paper))
                b.grid(row = table_num, column = 0)
    def show_paper_visual(self, window, paper, e = False):
        if not e:
            for c in window.winfo_children():
                c.destroy()
            window.title("Select page")
            l = Label(window, text = "Page number(1 - %d):"%(paper.num_pages))
            l.grid(row = 0, column = 0)
            e = Entry(window, width = 10)
            e.grid(row = 0, column = 1)
            e.focus_set()
            b = Button(window, text = "Proceed", command = lambda window = window, paper = paper, e = e: self.show_paper_visual(window, paper, e))
            b.grid(row = 1, column = 0)
            b = Button(window, text = "Cancel", command = lambda window = window, paper = paper: self.show_paper_info(window, paper))
            b.grid(row = 1, column = 1)
        else:
            number = e.get()
            if not number.isdigit():
                return 0
            else:
                number = int(number)
                if number < 1 or number > paper.num_pages:
                    return 0
                for c in window.winfo_children():
                    c.destroy()
                window.title("Visualization of %s (page %d)" % (paper.fname, number))
                cnvs = Canvas(window, width = 1200, height = 800)
                cnvs.grid(row = 0, column = 0, columnspan = 2)

                text = paper.get_xml_page(number, True)
                rows = analyze_page(text)
                max_width = max([row[-1].right - row[0].left for row in rows])
                header_row = False
                for row in rows:
                    if len(row) == 1 and row[0].text.startswith("Table "):
                        header_row = row
                        color = "red"
                    elif header_row and len(row) == 1 and abs(row[0].left - header_row[0].left) < 1.0:
                        color = "red"
                    elif abs(row[-1].right - row[0].left - max_width) < 1.0:
                        color = "blue"
                    else:
                        header_row = False
                        color = "black"
                    for l in row:
                        cnvs.create_rectangle((l.left, l.top + 10, l.right, l.bottom + 10), outline = color)
                
                b = Button(window, text = "Back", command = lambda window = window, paper = paper: self.show_paper_info(window, paper))
                b.grid(row = 1, column = 0)
    def export_paper_text(self, paper):
        # Export full text of a paper into a file.
        with asksaveasfile() as f:
            if f:
                text = paper.get_text()
                f.write(text)
    def export_texts(self):
        # Export texts of all papers into a directory. Each text is saved into a file named "papername.txt".
        d = askdirectory()
        for p in self.papers:
            text = p.get_text()
            with open(path_join(d, "%s.txt" % (p.fname)), "w") as f:
                f.write(text)
    def export_all(self, quick = False, n = None, n_p = None, errors = None, no_attr = None):
        # Export all papers' metadata. Quick export: determine metadata first if none is available, skipping all user interaction.
        if n is None:
            if self.unsaved_papers():
                if tkMessageBox.askyesno("Export", "Some papers were changed. These changes will be saved before export will be performed. Proceed?"):
                    self.save_paper()
                else:
                    return 0
            self.window.update_idletasks()
            n = 1
            n_p = 0
            errors = {}
            no_attr = {}
            no_attr["datasets"] = []
            for a in Paper.attributes_general:
                no_attr[a] = []
            self.window.after(100, lambda: self.export_all(quick, n, n_p, errors, no_attr))
        else:
            p = self.papers[n - 1]
            self.status_set("Performing export %d/%d. Paper: %s. Please, wait..." % (n, len(self.papers), p.fname))
            self.window.update_idletasks()
            try:
                outp = p.export(quick)
                if outp:
                    n_p += 1
                    for a in Paper.attributes_general:
                        if not outp["content"]["plain_text"][a]:
                            no_attr[a].append(p.fname)
                    if not outp["content"] or outp["content"].keys() == ["plain_text"]:
                        no_attr["datasets"].append(p.fname)
            except Exception as e:
                outp = False
                errors[p.fname] = e
            if n < len(self.papers):
                n += 1
                self.window.after(100, lambda: self.export_all(quick, n, n_p, errors, no_attr))
            else:
                msg = False
                if errors:
                    msg = "These papers were not exported for some reason:\n\n"
                    for e in errors:
                        msg += "%s : %s\n\n" % (e, errors[e])
                with open(NO_ATTRS_FILE, "w") as f:
                    for a in no_attr:
                        f.write("No %s: %d out of %d papers (%f%%) \n"%(a, len(no_attr[a]), n_p, float(len(no_attr[a])) / n_p * 100))
                        for p in no_attr[a]:
                            f.write(p + "\n")
                        f.write("\n")
                    if msg:
                        f.write(msg)
                self.status_set("")
                if msg:
                    tkMessageBox.showwarning("Unable to export papers", msg)

if __name__ == "__main__":
    root = Tk()
    mngr = Manager(root)
