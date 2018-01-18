"""
Functions for interacting with elastic search via python interface.
"""
import json
import os
import sys
import traceback

import elasticsearch
import elasticsearch.helpers

JSON_DIR = "D:/elasticsearch/out"
ANALYZER_OUT_DIR = "C:/Work/papers_analysis/manager/export"
# ANALYZER_OUT_DIR = "D:/elasticsearch/fake_export"


def index_get(es):
    """ Get all indices
    """
    print es.indices.get(index="*")


def index_create(es, index):
    """ Create a new index
    """
    # Note: it seems that mapping name and document_type are the same thing.
    if index == "supp-notes":
        # Supporting note
        body = {
                "mappings": {
                        "supp-note": {
                                "properties": {
                                        "dkbID": {"type": "keyword"},
                                        "GLANCE": {"type": "object"},
                                        "CDS": {"type": "object"}
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


def load_data(es):
    """ Load papers and support documents from JSONs.
    """
    results = []
    for i in range(0, 10):
        fname = os.path.join(JSON_DIR, "%d.json" % (i)).replace("\\", "/")
        with open(fname, "r") as f:
            doc = json.load(f)
        doc["_index"] = "papers"
        doc["_type"] = "paper"
        supp_notes = []
        print i
        for sn in doc["GLANCE"]["supporting_notes"]:
            sn["_index"] = "supp-notes"
            sn["_type"] = "supp-note"
            sn["datasets"] = {}
            supp_fname = os.path.join(ANALYZER_OUT_DIR,
                                      "%s.json" %
                                      (sn["dkbID"])).replace("\\", "/")
            if os.access(supp_fname, os.F_OK):
                with open(supp_fname, "r") as f:
                    content = json.load(f)["content"]
                print "Analyzed file found for %s, paper:%s" % (sn["dkbID"],
                                                                doc["dkbID"])
                for key in content:
                    if key.endswith("datasets"):
                        print "Datasets found for " + sn["dkbID"]
                        sn["datasets"][key] = content[key]
            supp_notes.append(sn)
            results.append(sn)
        doc["GLANCE"]["supporting_notes"] = [sn["dkbID"] for sn in supp_notes]
        results.append(doc)
    elasticsearch.helpers.bulk(es, results)


def doc_find_all(es, index="_all"):
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
                for category in hit["_source"]["datasets"]:
                    rtrn[2] += hit["_source"]["datasets"][category]
    return rtrn


def explore(es):
    contents = doc_find_all(es)
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
            if isinstance(r, dict) and w in r:
                r = r[w]
                path += " " + w
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
        try:
            load_data(es)
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
