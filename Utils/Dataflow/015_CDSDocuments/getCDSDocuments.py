#!/usr/bin/env python
"""
author: Maria Grigorieva
maria.grigorieva@cern.ch

refactored by: Golosova Marina
golosova.marina@gmail.com

Execute on local machine with installed invenio-client;
invenio-client also requires phantomjs (>=1.9.8 due to default SSL protocol)

input metadata:
  - list of papers & supporting documents [JSON] from GLANCE API

output metadata:
  1) CDS Records in JSON format
     <GLANCE_ID>.json

"""

import json
from urlparse import urlparse
import sys, getopt
import os

sys.path.append("../")
from pyDKB.dataflow import CDSInvenioConnector, KerberizedCDSInvenioConnector
from pyDKB.dataflow import dkbID, dataType

import warnings
from requests.packages.urllib3.exceptions import InsecurePlatformWarning


counter = 0

def usage():
    """ Output usage string. """
    msg = """
USAGE
  ./getCDSPapers.py <options> [file]

ARGUMENTS
  file                      Input file name (default: Input/list_of_papers.json)

OPTIONS
  -l, --login       LOGIN   CERN account login
  -p, --password    PASSWD  CERN account password
  -k, --kerberos            Use kerberos authorization

  -P, --pretty              Pretty print output

  -m, --mode        MODE    operating mode:
                            (f)ile  -- default mode: read from file,
                                       output to files
                            (s)tream -- stream mode: read from STDIN,
                                        output to STDOUT

  -o, --output-dir DIR      Output directory name

  -h, --help                Show this message and exit
"""
    sys.stderr.write(msg)


def collection_verification(collection):
    """ Check primary collection. """
    if len(collection) > 0 \
      and type(collection[0]) is dict \
      and collection[0]['primary'] in ('ARTICLE', 'ATLAS_Papers'):
        return True
    elif type(collection) is list:
        try:
            if collection[0]['primary'] in ('ARTICLE', 'ATLAS_Papers'):
                return True
        except (IndexError, TypeError, ValueError):
            return False


def search_paper(cds, paper_info):
    """ Perform CDS search by given paper info.

    Returns single JSON(dict) or None
    Search parameters:
          aas - advanced search ("0" means no, "1" means yes).  Whether
                search was called from within the advanced search
                interface.
          p1 - first pattern to search for in the advanced search
               interface.  Much like 'p'.

          f1 - first field to search within in the advanced search
               interface.  Much like 'f'.

          m1 - first matching type in the advanced search interface.
               ("a" all of the words, "o" any of the words, "e" exact
               phrase, "p" partial phrase, "r" regular expression).

          op1 - first operator, to join the first and the second unit
                in the advanced search interface.  ("a" add, "o" or,
                "n" not).
          p2 - second pattern to search for in the advanced search
               interface.  Much like 'p'.

          f2 - second field to search within in the advanced search
               interface.  Much like 'f'.

          m2 - second matching type in the advanced search interface.
               ("a" all of the words, "o" any of the words, "e" exact
                phrase, "p" partial phrase, "r" regular expression).

          op2 - second operator, to join the second and the third unit
                in the advanced search interface.  ("a" add, "o" or,
                "n" not).
          of - output format (e.g. "hb").
          cc - current collection (e.g. "ATLAS").  The collection the
               user started to search/browse from.
    """
    sys.stderr.write(paper_info["id"]+"\n")
    #results = cds.search(cc="ATLAS", aas=1, m1="e", op1="a",
    #                     p1=paper_info["full_title"], f1="title", m2="a",
    #                     op2="a", p2="ARTICLE, ATLAS_Papers",
    #                     f2="collection", m3="a", p3=paper_info["ref_code"],
    #                     f3="report_number", of="recjson")
    results = cds.search(cc="ATLAS", aas=1, m1="p", p1=paper_info["ref_code"],
                         f1="reportnumber", m2="a", op2="a",
                         p2="ARTICLE, ATLAS_Papers", f2="collection",
                         of="recjson")
    try:
        res = json.loads(results)
        if type(res) == list:
            if len(res) > 1:
                sys.stderr.write("(WARN) Paper search returned more than one"
                                 " result (%s)\n"
                                 "(WARN) Will be taken the first of the"
                                 " list\n" % str(len(res)))
            r = None
            for item in res:
                if collection_verification(item.get("collection")):
                    r = item
                    break
            res = r
        if type(res) == dict:
            res['glance_id'] = paper_info["id"]
        else:
            sys.stderr.write("(WARN) Paper search result is of wrong type"
                             " (expected %s, get %s)\n" % (dict, type(res)))
            res = None
        return res
    except ValueError:
        sys.stderr.write("Decoding JSON has failed\n")
        return None

def search_notes(cds, notes):
    """ Get NOTES metadata from CDS.

    NOTES is a JSON (dict) array with supporting documents information.
    Returns dict: { (str) note_id : (dict|NoneType) note_metadata}
    """
    if notes == None:
        return {}
    if type(notes) != list:
        return None
    results = {}
    for note in notes:
        results[note.get("id")] = search_note(cds, note)

    return results

def search_note(cds, note):
    """ Get NOTE metadata from CDS.

    NOTE is a JSON node (dict) with a single supporting document information.
    Returns (dict|NoneType) note_metadata
    """
    global counter
    if type(note) != dict:
        return None
    url = note.get("url", None)
    if not url:
        return None
    url = url.replace('\\', '')
    parsed = urlparse(url)
    if parsed.netloc == 'cds.cern.ch' or parsed.netloc == 'cdsweb.cern.ch':
        sys.stderr.write(parsed.path+"\n")
        if parsed.path[-1:] == '/':
            recid = parsed.path[:-1].split('/')[-1]
        else:
            recid = parsed.path.split('/')[-1]
        counter += 1
        sys.stderr.write(str(counter) + ' : ' + str(recid) + "\n")

        # metadata from CDS Invenio in json format
        results = cds.search(recid=recid, of="recjson")

        try:
            results = json.loads(results)
        except ValueError:
            if "You are not authorized to perform this action" in results:
                sys.stderr.write("This Supporting Document is not available"
                                 " for your user.\n")
            elif "Sign in with a CERN account, a Federation account or" \
                 " a public service account" in results:
                sys.stderr.write("This Supporting Document is not available"
                                 " for unauthenticated user. Specify"
                                 " login/password or use Kerberos"
                                 " authentication.\n")
            else:
                sys.stderr.write("JSON decoding failed.\n")
            results = None

    else:
        results = None

    if type(results) == list:
        if len(results) > 1:
            sys.stderr.write("(WARN) Supporting document search returned more"
                             " than one result (%s)\n"
                             "(WARN) Will be taken the first of the list\n"
                              % str(len(results)))
        results = results[0]
    if type(results) != dict:
        results = None

    return results

def form_output_data(GLANCEdata, ppCDSdata, sdCDSdata):
    """ Combine input and found metadata; generate and add dkbID.

    Parameters:
      GLANCEdata -- main input JSON data
      ppCDSdata  -- paper information from CDS
      sdCDSdata  -- supporting documents information from CDS:
                    { $sdGlanceID : [ ... ], ... }
    """
    if type(ppCDSdata) != type(GLANCEdata) != type(sdCDSdata) != dict:
        sys.stderr.write("(ERROR) form_output_data() expected parameters of"
                         " type %s (get %s, %s, %s)\n" % (dict,
                         type(GLANCEdata), type(ppCDSdata), type(sdCDSdata)))
    result = {}

    ppGLANCEdata = GLANCEdata.copy()
    if ppGLANCEdata.get("supporting_notes"):
        sdGLANCEdata = ppGLANCEdata.pop("supporting_notes")
    else:
        sdGLANCEdata = []

    result["GLANCE"] = ppGLANCEdata
    result["CDS"] = ppCDSdata
    result["dkbID"] = dkbID(result, dataType.DOCUMENT)

    sd_results = []
    result["supporting_notes"] = sd_results

    if type(sdGLANCEdata) != list:
        sys.stderr.write("(WARN) GLANCE info for supporting_notes supposed to"
                         " be of type %s (get %s)\n"
                         % (list, type(sdGLANCEdata)))
        return result

    for glance_ind in range(len(sdGLANCEdata)):
        sd_result = {}
        glance_item = sdGLANCEdata[glance_ind]
        sd_result["GLANCE"] = glance_item

        cds_item = sdCDSdata.get(glance_item.get("id"))
        if not cds_item:
            sys.stderr.write("(WARN) No CDS data for GLANCE id: %s\n"
                             % glance_item.get("id"))
            continue
        if type(cds_item) != dict:
            sys.stderr.write("(WARN) CDS item for GLANCE id: %s is of wrong"
                             " type (expected '%s', get '%s')\n"
                             % (glance_item.get("id"), dict, type(cds_item)))
            continue

        sd_result["CDS"] = cds_item
        sd_result["dkbID"] = dkbID(sd_result, dataType.DOCUMENT)
        sd_results.append(sd_result)

    return result


def input_json_handle(json_data, cds):
    """ Process input JSON data (taken from file or a stream).

    Returns resulting JSON for output.
    Returns None if nothing was found in CDS.
    """
    ds_results = search_notes(cds, json_data.get("supporting_notes", None))
    pp_results = search_paper(cds, json_data)
    if type(ds_results) != dict:
        ds_results = {}
    if type(pp_results) != dict:
        pp_results = {}
    if not pp_results and not ds_results:
        return None
    result = form_output_data(json_data, pp_results, ds_results)
    return result


def input_file_handle(fname, cds, indent, out_dir="./"):
    """ Process input file. """
    try:
        data_file = open(fname)
    except IOError, e:
        sys.stderr.write("ERROR: %s: %s\n" % (fname, e.strerror))
        sys.exit(e.errno)
    else:
        with data_file:
            data = json.load(data_file)

    for item in data:
        sys.stderr.write(item["id"]+"\n")
        result = input_json_handle(item, cds)
        if not result:
            continue
        f = open(out_dir + "/%s.json" % item["id"], "w")
        json.dump(result, f, indent=indent)
        f.close()

    sys.stderr.write("done!\n")

def input_stream_handle(stream, cds):
    """ Process input stream. """
    if type(stream) != file:
        sys.stderr.write("ERROR: input_stream_handle: expected <file>,"
                         " got %s.\n" % type(stream))
        return False
    if stream.closed:
        sys.stderr.write("ERROR: input_stream_handle:"
                         " <file> is already closed.\n")
        return False

    instream = iter(stream.readline, '')

    for raw_item in instream:
        try:
            item = json.loads(raw_item)
        except ValueError:
            sys.stderr.write("WARNING: can't decode input line as JSON."
                             " Skipping.\n")
            continue

        result = input_json_handle(item, cds)
        if not result:
            continue
        sys.stdout.write(json.dumps(result)+"\n")

        # Shell we mark the "end-of-processing" even when no data found?..
        sys.stdout.write("\0")
        sys.stdout.flush()

def main(argv):
    """ Program body. """
    login = ''
    password = ''
    kerberos = False
    mode = 'f'
    indent = None
    out_dir = "./"
    try:
        opts, args = getopt.getopt(argv, "hl:p:km:Po:",
             ["login=", "password=", "kerberos", "mode=", "pretty",
              "output-dir"])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-l", "--login"):
            login = arg
        elif opt in ("-p", "--password"):
            password = arg
        elif opt in ("-k", "--kerberos"):
            kerberos = True
        elif opt in ("-m", "--mode"):
            mode = arg
        elif opt in ("-P", "--pretty"):
            indent = 2
        elif opt in ("-o", "--output-dir"):
            out_dir = arg

    if len(args) == 0:
        infile = 'Input/list_of_papers.json'
    elif len(args) == 1:
        infile = args[0]
    else:
        usage()

    if not os.path.isdir(out_dir):
        sys.stderr.write("Creating output directory...\n")
        try:
            os.mkdir(out_dir)
        except OSError, e:
            sys.stderr.write("ERROR: Failed to create output directory: %s\n"
                             % e)
            sys.stderr.write("Output to the current dir instead.\n")
            out_dir = "./"

    if not login and not kerberos:
        sys.stderr.write("WARNING: no authentication method will be used.\n")

    warnings.simplefilter("once", InsecurePlatformWarning)

    if kerberos:
        Connector = KerberizedCDSInvenioConnector
    else:
        Connector = CDSInvenioConnector

    with Connector(login, password) as cds:

        if mode in ("f", "file"):
            input_file_handle(infile, cds, indent, out_dir)

        elif mode in ("s", "stream"):
            input_stream_handle(sys.stdin, cds)

        else:
            sys.stderr.write("Wrong value for MODE parameter: %s\n" % mode)
            usage()
            exit(2)

if __name__ == "__main__":
    main(sys.argv[1:])
