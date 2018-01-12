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
rm = ["authors", "filenames", "files", "filetypes", "periodical_internal_note",
      "restriction_access"]
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


def purge(inp):
    """ Remove keys from dictionary, repeat for contents recursively.

    inp - input dictionary (or list which contains dictionaries)
    """
    if isinstance(inp, dict):
        d = {}
        for key in inp:
            if key not in rm:
                if isinstance(inp[key], dict):
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


def purge_store(j, fname):
    """ Use purge() on a json, then save it to a file.

    j - json
    fname - name of file where the json should be stored.
    NOTE: file is overwritten.
    """
    jsn = purge(j)
    with open(fname, "w") as f:
        json.dump(jsn, f, indent=4)


if __name__ == "__main__":
    fname = "D:/elasticsearch/document-metadata"
    fname_out = "D:/elasticsearch/out.json"
    dirname_out = "D:/elasticsearch/out"
    json_nd = True
    if not fname:
        if len(sys.argv) < 2:
            sys.exit(0)
        fname = sys.argv[1]
    if json_nd:
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
                fname_out = os.path.join(dirname_out,
                                         "%d.json" % (i)).replace("\\", "/")
                purge_store(j, fname_out)
            except Exception as e:
                sys.stderr.write("EXCEPTION " + str(e) + ", details:" +
                                 traceback.format_exc() + "\n")
            i += 1
##            if i == 10:
##                break
    else:
        with open(fname, "r") as f:
            try:
                j = json.load(f)
            except Exception as e:
                sys.stderr.write(str(e))
                sys.exit(1)
            purge_store(j, fname_out)
