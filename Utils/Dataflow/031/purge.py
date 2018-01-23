"""
Script for removing keys and some other things from JSONs.
"""

import json
import os
import sys
import traceback


"""
rm - list of keys which should be removed.
renames - parameters for simplifying layered dictionaries. Example:
renames = {
        "a":["b","a and b"]
    }
result:
{
    "a":{"b":"c", "d":"e"}
}
changes into
{
    "a and b":"c"
}
This also works for arrays of dictionaries:
{"a":[{"b":1}, {"b":2}, {"c":3}]}
changes into
{"a and b":[1, 2]}
"""
rm = ["authors", "copyright_status", "filenames", "files", "filetypes",
      "license", "periodical_internal_note", "restriction_access"]
renames = {
        "email_message": ["address", "email_address"],
        "abstract": ["summary", "abstract"],
        "imprint": ["date", "imprint_date"],
        "title": ["title", "title"],
        "title_additional": ["title", "title_additional"],
        "system_number": ["value", "system_number"],
        "collection": ["primary", "collection"],
        "physical_description": ["pagination", "pagination"],
        "corporate_name": ["collaboration", "collaboration"],
        "keywords": ["term", "keywords"],
    }


def add_vlk(d, key, ls):
    """ Add/replace a new key into a dictionary according to ES's
    mapping rules.

    d - dictionary
    key - key name
    ls - value, type = list

    If ls is empty, key is not added.
    If ls contains exactly one element, key is added,
    value is added as a single element.
    If ls contains several elements, key is added,
    value is added as list.
    Example:
    {}, "k", [] => d = {}
    {}, "k", [0] => d = {"k":0}
    {}, "k", [0, 1, 2, 3] => d = {"k":[0, 1, 2, 3]}
    """
    if len(ls) > 1:
        d[key] = ls
    elif len(ls) == 1:
        d[key] = ls[0]


def purge(inp):
    """ Remove keys from dictionary, repeat for contents recursively.

    inp - input dictionary (or list which contains dictionaries)
    """
    if isinstance(inp, dict):
        d = {}
        for key in inp:
            if key not in rm:
                if key.lower() == "report_number":
                    if isinstance(inp[key], dict):
                        for key2 in inp[key]:
                            if key2.lower() == "internal":
                                d["internal_number"] = inp[key][key2]
                            elif key2.lower() == "report_number":
                                d["report_number"] = inp[key][key2]
                    elif isinstance(inp[key], list):
                        int_nums = []
                        rep_nums = []
                        for d2 in inp[key]:
                            for key2 in d2:
                                if key2.lower() == "internal":
                                    int_nums.append(d2[key2])
                                elif key2.lower() == "report_number":
                                    rep_nums.append(d2[key2])
                        add_vlk(d, "internal_number", int_nums)
                        add_vlk(d, "report_number", rep_nums)
                    else:
                        d[key] = inp[key]
                elif key.lower() == "system_control_number":
                    if isinstance(inp[key], dict):
                        for key2 in inp[key]:
                            if key2.lower() == "institute":
                                if inp[key][key2].lower() == "inspire":
                                    d["inspire_number"] = inp[key]["value"]
                                elif inp[key][key2].lower() == "arxiv":
                                    d["report_number"] = inp[key]["value"]
                    elif isinstance(inp[key], list):
                        ins_nums = []
                        arx_nums = []
                        for d2 in inp[key]:
                            for key2 in d2:
                                if key2.lower() == "institute":
                                    if d2[key2].lower() == "inspire":
                                        ins_nums.append(d2["value"])
                                    elif d2[key2].lower() == "arxiv":
                                        arx_nums.append(d2["value"])
                        add_vlk(d, "inspire_number", ins_nums)
                        add_vlk(d, "arxiv_number", arx_nums)
                    else:
                        d[key] = inp[key]
                elif isinstance(inp[key], dict):
                    if key in renames and renames[key][0] in inp[key]:
                        d[renames[key][1]] = inp[key][renames[key][0]]
                    else:
                        d[key] = inp[key]
                elif isinstance(inp[key], list) and inp[key]:
                    if key in renames:
                        arr_of_dicts = True
                        for i in inp[key]:
                            if not isinstance(i, dict):
                                arr_of_dicts = False
                                break
                        if arr_of_dicts:
                            new_arr = []
                            for i in inp[key]:
                                if renames[key][0] in i:
                                    new_arr.append(i[renames[key][0]])
                            d[renames[key][1]] = new_arr
                        else:
                            d[key] = inp[key]
                    else:
                        d[key] = inp[key]
                elif key == "clasification_number":
                    d["classification_number"] = inp[key]
                else:
                    d[key] = inp[key]
        for key in d:
            if isinstance(d[key], dict) or isinstance(d[key], list):
                d[key] = purge(d[key])
        outp = d
    elif isinstance(inp, list):
        arr = list(inp)
        for i in range(0, len(arr)):
            if isinstance(arr[i], dict) or isinstance(arr[i], list):
                arr[i] = purge(arr[i])
        outp = arr
    return outp


def purge_store(j, outf):
    """ Use purge() on a JSON data, then save it to a file.

    j - JSON data
    outf - file where the data should be stored. If outf is a string, then it
    is presumed to be a name for a new JSON file to be created. If outf is a
    file object, then it is presumed to be an already opened NDJSON.
    NOTE: file is overwritten in first case.
    """
    jsn = purge(j)
    if "supporting_notes" in jsn and "GLANCE" in jsn:
        jsn["GLANCE"]["supporting_notes"] = jsn["supporting_notes"]
        del jsn["supporting_notes"]
    if isinstance(outf, str) or isinstance(outf, unicode):
        with open(outf, "w") as f:
            json.dump(jsn, f, indent=4)
    elif isinstance(outf, file):
        outf.write(json.dumps(jsn) + "\n")


if __name__ == "__main__":
    with open("config.json", "r") as f:
        config = json.load(f)
    fname = config["FNAME"]
    fname_out = config["FNAME_OUT"]
    dirname_out = config["DIRNAME_OUT"]
    json_nd_in = config["JSON_ND_IN"]
    json_nd_out = config["JSON_ND_OUT"]
    if not fname:
        if len(sys.argv) < 2:
            sys.exit(0)
        fname = sys.argv[1]
    if json_nd_in:
        if json_nd_out:
            outf = open(fname_out, "w")
        with open(fname, "r") as f:
            lines = f.readlines()
        try:
            os.mkdir(dirname_out)
        except Exception as e:
            pass
        i = 0
        for line in lines:
            print i
# This is copied from manager.py function. Could be imported.
            try:
                j = json.loads(lines[i])
                if not json_nd_out:
                    outf = os.path.join(dirname_out,
                                        "%d.json" % (i)).replace("\\", "/")
                purge_store(j, outf)
            except Exception as e:
                sys.stderr.write("EXCEPTION " + str(e) + ", details:" +
                                 traceback.format_exc() + "\n")
            i += 1
##            if i == 10:
##                break
        if json_nd_out:
            outf.close()
    else:
        with open(fname, "r") as f:
            try:
                j = json.load(f)
            except Exception as e:
                sys.stderr.write(str(e))
                sys.exit(1)
            purge_store(j, fname_out)
