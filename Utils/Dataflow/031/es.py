"""
Functions for interacting with elastic search via python interface.
"""
import json
import os
import sys
import traceback

import elasticsearch
import elasticsearch.helpers

EXPLORE_METAFIELDS = False
JSON_DIR = "D:/elasticsearch/out"


def index_get(es):
    """ Get all indices
    """
    print es.indices.get(index="*")


def index_create(es, index):
    """ Create a new index
    """
    # Note: it seems that mapping name and document_type are the same thing.
    if index == "supp-notes":
        with open("PDFAnalyzer_mapping.json", "r") as f:
            PDFA = json.load(f)
        # Supporting note
        body = {
                "mappings": {
                        "supp-note": {
                                "properties": {
                                        "dkbID": {"type": "keyword"},
                                        "GLANCE": {"type": "object"},
                                        "CDS": {"type": "object"},
                                        "PDFAnalyzer": {"properties": PDFA}
                                    }
                            }
                    }
            }
    elif index == "papers":
        with open("mapping.json", "r") as f:
            paper_properties = json.load(f)
        # Paper
        body = {
                "mappings": {
                        "paper": {
                                "dynamic": "strict",
                                "properties": paper_properties
                            }
                    }
            }
    else:
        return False
    es.indices.create(index=index, body=body)


def index_delete(es, index):
    """ Delete an index
    """
    es.indices.delete(index=index, ignore=[404])


def doc_add(es, index, mapping):
    """ Add document
    """
    doc = {
            "dkbID": "A1",
            "supp-notes": [{"dkbID": "CDS_CERN-ATL-COM-PHYS-2011-684"}]
        }
    es.index(index=index, doc_type=mapping, body=doc)


def doc_del_by_query(es, index="_all"):
    """ Delete all documents matching query
    """
    srch = {
            "query": {
                    "match_all": {}
                }
        }
    es.delete_by_query(index=index, body=srch)


def load_data(es, source, pdfdata):
    """ Load papers and support documents from JSONs.

    es - elasticsearch handle.
    source - source of data.
    pdfdata - directory with files exported from PDF Analyzer.
    """
    results = []
    docs = []
    if isinstance(source, str) or isinstance(source, unicode):
        i = 0
        while True:
            try:
                fname = os.path.join(JSON_DIR, "%d.json" %
                                     (i)).replace("\\", "/")
                sys.stderr.write("Loading %s..." % fname)
                with open(fname, "r") as f:
                    docs.append(json.load(f))
                sys.stderr.write("Done.\n")
                i += 1
            except Exception as e:
                sys.stderr.write("Unable to load %s, proceeding.\n" % fname)
                break
    elif isinstance(source, file):
        lines = source.readlines()
        source.close()
        for line in lines:
            docs.append(json.loads(line))
    else:
        return False
    for doc in docs:
        doc["_id"] = doc["dkbID"]
        doc["_index"] = "papers"
        doc["_type"] = "paper"
        supp_notes = []
        try:
            doc["CDS"]["publication_info"]["volume"] = \
                    int(doc["CDS"]["publication_info"]["volume"])
        except Exception as e:
            pass
        for sn in doc["GLANCE"]["supporting_notes"]:
            sn["_id"] = sn["dkbID"]
            sn["_index"] = "supp-notes"
            sn["_type"] = "supp-note"
            supp_fname = os.path.join(pdfdata, "%s.json" %
                                      (sn["dkbID"])).replace("\\", "/")
            if os.access(supp_fname, os.F_OK):
                with open(supp_fname, "r") as f:
                    sn["PDFAnalyzer"] = json.load(f)["content"]
                if "plain_text" in sn["PDFAnalyzer"] \
                        and "links" in sn["PDFAnalyzer"]["plain_text"] \
                        and "" in sn["PDFAnalyzer"]["plain_text"]["links"]:
                    sn["PDFAnalyzer"]["plain_text"]["links"]["NO_NAME"] =\
                            sn["PDFAnalyzer"]["plain_text"]["links"][""]
                    del sn["PDFAnalyzer"]["plain_text"]["links"][""]
            else:
                sn["PDFAnalyzer"] = {}
            supp_notes.append(sn)
            results.append(sn)
        doc["GLANCE"]["supporting_notes"] = [sn["dkbID"] for sn in supp_notes]
        results.append(doc)
    elasticsearch.helpers.bulk(es, results)


def doc_find_all(es, index="_all", meta=False):
    """ Find all documents
    """
    srch = {
            "query": {
                    "match_all": {}
                }
        }
    rtrn = {}
    fr = 0
    sz = 100
    while True:
        results = es.search(index=index, body=srch, from_=fr, size=sz)
        if not results["hits"]["hits"]:
            break
        for hit in results["hits"]["hits"]:
            if hit["_index"] not in rtrn:
                rtrn[hit["_index"]] = []
            if meta:
                rtrn[hit["_index"]].append(hit)
            else:
                rtrn[hit["_index"]].append(hit["_source"])
        fr += sz
    return rtrn


def doc_find_by_query(es, paperid):
    srch = {
            "query": {
                    "match": {"dkbID": paperid}
                }
        }
    rtrn = [False, [], []]
    r = es.search(index="papers", body=srch)
    if r["hits"]["hits"]:
        sdocs = []
        datasets = []
        for hit in r["hits"]["hits"]:
            for sd in hit["_source"]["GLANCE"]["supporting_notes"]:
                sdocs.append(sd)
        # For now we believe that exact paper name was typed, and it's unique.
        rtrn[0] = r["hits"]["hits"][0]["_source"]

        srch = {
                "query": {
                        "terms": {"dkbID": sdocs}
                    }
            }
        r = es.search(index="supp-notes", body=srch)
        if r["hits"]["hits"]:
            for hit in r["hits"]["hits"]:
                rtrn[1].append(hit["_source"])
                for key in hit["_source"]["PDFAnalyzer"]:
                    if key.endswith("datasets"):
                        rtrn[2] += hit["_source"]["PDFAnalyzer"][key]
    return rtrn


def explore(es):
    contents = doc_find_all(es, meta=EXPLORE_METAFIELDS)
    if not contents:
        print "No data to explore"
        return False
    r = contents
    path = "ALL"
    show_all = False
    while True:
        print "Path:" + path
        if isinstance(r, dict):
            print r.keys()
        elif isinstance(r, list):
            print "%d element(s) in list" % (len(r))
            if show_all:
                print r
            show_all = False
        else:
            print r
        s = raw_input("Show:")
        if not s:
            break
        r = contents
        path = "ALL"
        words = s.split()
        for w in words:
            if isinstance(r, dict):
                if w in r:
                    r = r[w]
                    path += " " + w
                else:
                    for key in r:
                        if str(key).startswith(w):
                            r = r[key]
                            path += " " + key
                            break
            elif isinstance(r, list) and w == "all":
                show_all = True
                break
            elif isinstance(r, list) and int(w) < len(r):
                r = r[int(w)]
                path += " " + w
            else:
                break


if __name__ == "__main__":
    es = elasticsearch.Elasticsearch(["localhost:9200"])
    try:
        cmnd = sys.argv[1]
        args = sys.argv[2:]
    except Exception as e:
        sys.stderr.write("Wrong command line parameters:")
        sys.stderr.write(str(e))
        sys.exit(1)
    if not args:
        key = False
    else:
        key = args[0]
    if cmnd == "explore":
        explore(es)
    elif cmnd == "reindex":
        index_delete(es, "papers")
        index_delete(es, "supp-notes")
        index_create(es, "papers")
        index_create(es, "supp-notes")
    elif cmnd == "load":
        with open("config.json", "r") as f:
            config = json.load(f)
        if config["JSON_ND_OUT"]:
            source = open(config["FNAME_OUT"], "r")
        else:
            source = config["DIRNAME_OUT"]
        try:
            load_data(es, source, config["PDF_DATA"])
        except Exception as e:
            sys.stderr.write("EXCEPTION " +
                             ", details:" + traceback.format_exc() + "\n")
            print e[1][0]["index"]["data"]["dkbID"]
            print e[1][0]["index"]["data"]
            print e[1][0]["index"]["error"]
    elif cmnd == "find":
        [paper, suppnotes, datasets] = doc_find_by_query(es, key)
        if paper:
            print "Paper:" + paper["dkbID"] + str(paper)[:100] + "...}"
            print "Supporting notes:"
            for note in suppnotes:
                print note["dkbID"] + str(note)[:100] + "...}"
            print "Datasets:"
            print datasets
        else:
            print "No such paper"
