'''
author: Maria Grigorieva
maria.grigorieva@cern.ch

input :
  - directory with SupportingDocuments JSON-files

output :
  - directory with SupportingDocuments TTL-files

'''
import os
import json
import urllib
import urllib2
import httplib2
import uuid
import sys
import getopt
import re


ontology_params = {'ontology'         : 'http://nosql.tpu.ru/ontology/ATLAS',
                   'graph'            : 'http://nosql.tpu.ru:8890/DAV/ATLAS/new',
                   'rdf_prefix'       : 'http://www.w3.org/1999/02/22-rdf-syntax-ns',
                   'person_resource'  : 'http://nosql.tpu.ru:8890/DAV/ATLAS/person/',
                   'document_resource': 'http://nosql.tpu.ru:8890/DAV/ATLAS/document/'}

SPARQL                = "http://nosql.tpu.ru:8890/sparql"

#-----------------------------------------------------------------------------------------------------------
# Lists of dictionaries with parameters names for JSON documents and Ontology representation
#-----------------------------------------------------------------------------------------------------------
author_params_list = [{'CDS'   : 'INSPIRE_number',
                       'SPARQL': 'INSPIRE_number',
                       'ONTO'  : 'hasINSPIRENumber',
                       'unique': True},
                      {'CDS'   : 'control_number',
                       'SPARQL': 'control_number',
                       'ONTO'  : 'hasControlNumber',
                       'unique': True},
                      {'CDS'   : 'e-mail',
                       'SPARQL': 'email',
                       'ONTO'  : 'hasEmail',
                       'unique': True},
                      {'CDS'   : ('affiliation', 'first_name', 'last_name'),
                       'SPARQL': ('affiliation', 'first_name', 'last_name'),
                       'ONTO'  : ['hasAffilation', 'hasFirstName, hasLastName'],
                       'unique': True},
                      ]

author_attrs = [{'CDS': 'INSPIRE_number',  'SPARQL': 'INSPIRE_number',    'ONTO': 'hasINSPIRENumber'},
                {'CDS': 'control_number',  'SPARQL': 'control_number',    'ONTO': 'hasControlNumber'},
                {'CDS': 'e-mail',          'SPARQL': 'email',             'ONTO': 'hasEmail'},
                {'CDS': 'first_name',      'SPARQL': 'first_name',        'ONTO': 'hasFirstName'},
                {'CDS': 'last_name',       'SPARQL': 'last_name',         'ONTO': 'hasLastName'},
                {'CDS': 'affiliation',     'SPARQL': 'affiliation',       'ONTO': 'hasAffiliation'},
                ]


paper_attrs = [{'CDS': 'creation_date',         'SPARQL': 'creation_date',      'ONTO': 'hasCreationDate'},
               #{'CDS': 'CDS_ReportNumber',      'SPARQL': 'CDS_ReportNumber',   'ONTO': 'hasCDSReportNumber'},
               {'CDS': 'CDSInternal',           'SPARQL': 'CDSInternal',        'ONTO': 'hasCDSInternal'},
               {'CDS': 'CDS_ID',                'SPARQL': 'CDS_ID',             'ONTO': 'hasCDS_ID'},
               {'CDS': 'abstract',              'SPARQL': 'abstract',           'ONTO': 'hasAbstract'},
               {'CDS': 'title',                 'SPARQL': 'title',              'ONTO': 'hasFullTitle'},]

#-----------------------------------------------------------------------------------------------------------
# SPARQL Queries
#-----------------------------------------------------------------------------------------------------------

sparql_query = '''
                WITH <{graph}> SELECT ?guid, ?{param_name}
                WHERE {{
                    ?guid <{ontology}#{ONTO}> ?{param_name} .
                    FILTER(?{param_name} IN ({params_list}))
                }}
            '''

_sparql_get_documentGUID = '''WITH <{graph}> SELECT ?document
                          WHERE {{
                            ?document <{ontology}#hasGLANCE_ID> {GLANCE_ID} .
                            ?document <{rdf_prefix}#type> <{ontology}#SupportingDocument> .
                          }}'''



_sparql_authors  = '''WITH <{graph}> SELECT ?guid, ?first_name, ?last_name, ?affiliation
                     WHERE {{
                        ?guid <{ontology}#hasFirstName> ?first_name .
                        ?guid <{ontology}#hasLastName> ?last_name .
                        OPTIONAL {{?guid <{ontology}#hasAffilation> ?affiliation }}.
                        FILTER (?last_name = "{last_name}") .
                        FILTER (?first_name IN ("{first_letter}","{first_name}")) .
                        OPTIONAL {{ FILTER (?affiliation = "{affiliation}") }} .
                     }}'''


#-----------------------------------------------------------------------------------------------------------
# TTL Strings for Virtuoso
#-----------------------------------------------------------------------------------------------------------

_ttl_keyword = '''<{docGUID}> <{ontology}#hasKeyword> "{keyword}" .
'''

invalid_escape = re.compile(r'\\[0-7]{1,3}')  # up to 3 digits for byte values up to FF

def usage():
  msg='''
USAGE
  ./getCDSPapers.py <options>

OPTIONS
  -l, --login     LOGIN      VIRTUOSO Conductor's login
  -p, --passwd    PASSWORD   VIRTUOSO Conductor's password
  -i, --input     INPUT      path to input JSON-file
  -o, --output    OUTPUT     output directory for TTL files [with final slash "/.../.../"]
  -m, --mode      MODE       operating mode:
                                f|file   -- default mode: read from file,
                                                      output to files
                                s|stream -- stream mode: read from STDIN,
                                                     output to STDOUT
                                t|test   -- test mode: read from -i directory,
                                                       output to -o directory

  -h, --help                 Show this message and exit
'''
  sys.stderr.write(msg)


def main(argv):
    login    = ""
    password = ""
    input    = ""
    output   = ""

    try:
        opts, args = getopt.getopt(argv, "hl:p:i:o:m:", ["login=", "passwd=", "input=", "output=", "mode="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    # Default parameters
    mode = "file"

    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-l", "--login"):
            login = arg
        elif opt in ("-p", "--passwd"):
            password = arg
        elif opt in ("-i", "--input"):
            input = arg
        elif opt in ("-o", "--output"):
            output = arg
        elif opt in ("-m", "--mode"):
            mode = arg

    h = httplib2.Http()
    h.add_credentials(login, password)

    if mode in ('f', 'file'):
        with open(input, 'rb+') as data_file:
            content = data_file.read()
            filename = os.path.basename(input)
            jsons = fix_improper_JSON_content(content)
            print jsons
            glance_id = str(filename.split('.')[0])
            for i in jsons:
                try:
                    json_processing(glance_id, json.loads(i), output + str(uuid.uuid4()) + '.ttl')
                    sys.stderr.write("done!\n")
                except:
                    sys.stderr.write("Improper JSON!\n")
            sys.stderr.write("done!\n")

    # USAGE in CMD:
    # variable = $(cat <<SETVAR
    # .......JSON_CONTENT ....
    # SETVAR)
    #
    # echo "$variable" | python papers2TTL.py -l ... -p ... -m s )
    elif mode in ('s', 'stream'):
        data_stream = sys.stdin.read()
        json_processing(fix_JSON(data_stream), output)
        sys.stderr.write("done!\n")

    elif mode in ('t', 'test'):
        dir_name = os.path.dirname(input)
        for filename in os.listdir(dir_name):
            with open(dir_name +"/"+ filename, 'r') as data_file:
                sys.stderr.write("Reading file " + str(filename))
                content = data_file.read()
                jsons = fix_improper_JSON_content(content)
                glance_id = str(filename.split('.')[0])
                for i in jsons:
                    try:
                       json_processing(glance_id, json.loads(i), output + str(uuid.uuid4()) + '.ttl')
                       sys.stderr.write("done!\n")
                    except:
                        sys.stderr.write("Improper JSON!\n")
    else:
        sys.stderr.write("Wrong value for MODE parameter: {m}\n".format(m=mode))
        usage()
        exit(2)

# write ttl string with document metadata to TTL file
def writeTTL2file(output, ttl_string):
    try:
        ttl_file = open(output, "w+")
    except IOError:
        sys.stderr.write('cannot open file')
    else:
        try:
            ttl_file.write(ttl_string)
            ttl_file.write('\n')
        except IOError:
            sys.stderr.write('can\'t write to file')
        else:
            ttl_file.close()
            sys.stderr.write("TTL file has written!")

def json_processing(glance_id, data, output):
    if (isSupportingDocument(data[0])):
        print "Support Document!\n"

        #authors_processing(data[0])
        # print "Authors processed"
        ttl_string = json2TTL(glance_id, data[0])
        print "TTL_STRING "
        print ttl_string
        if ttl_string:
            if output != '':
                writeTTL2file(output, ttl_string)
            else:
                sys.stdout.write(ttl_string + "\n\0")
                sys.stdout.flush()

#--------------------------------------------------------------------------------------------------------------------
# DATABASE FUNCTIONS
#--------------------------------------------------------------------------------------------------------------------
# execute SPARQL requests with urllib2 library
def sparqlQuery(query, baseURL, format="application/sparql-results+json"):
    params = {
        "query": query,
        "format": format
    }
    querypart = urllib.urlencode(params)
    try:
        response = urllib.urlopen(baseURL, querypart).read()
    except urllib2.HTTPError as e:
        sys.stderr.write('The server couldn\'t fulfill the request.')
        sys.stderr.write('Error code: '), e.code
    except urllib2.URLError as e:
        sys.stderr.write('We failed to reach a server.')
        sys.stderr.write('Reason: '), e.reason
    else:
        return json.loads(response)

def getAuthorsByParameter(param_name, ONTO, params_list):
    query = sparql_query.format(param_name=param_name,ONTO=ONTO,params_list=params_list, **ontology_params)
    results = sparqlQuery(query, SPARQL)['results']['bindings']
    # print "results length = " + str(len(results))
    return results

def getAuthorsByFLA(first_name, last_name, first_letter, affiliation):
    query = _sparql_authors.format(first_name=first_name,last_name=last_name,
                                   first_letter=first_letter,affiliation=affiliation, **ontology_params)
    return sparqlQuery(query, SPARQL)['results']['bindings']

# check if Virtuoso
def hasResults(results):
    if type(results['results']['bindings']) is list:
        length = len(results['results']['bindings'])
    return True if length > 0 else False

# search document in Virtuoso by GLANCE_ID and document type = SupportingDocument
def getDocumentGUID(GLANCE_ID):
    query = _sparql_get_documentGUID.format(GLANCE_ID=GLANCE_ID, **ontology_params)
    results = sparqlQuery(query, SPARQL)['results']['bindings']
    print results
    return results[0]['document']['value'] if len(results) > 0 else None

# check 'collection' parameter from JSON-string
# defining document type
# return TRUE if supporting document
def isSupportingDocument(data):
    if 'collection' in data:
        collection = data['collection']
        if type(collection) is list:
            for item in collection:
                return re.match(
                    'INTNOTEATLASPRIV|Internal\sNotes|INTNOTEATLASPUBL|INTNOTEATLASINT|InternalNote',
                    str(item['primary']))
        elif type(collection) is dict:
            return re.match('INTNOTEATLASPRIV|Internal\sNotes|INTNOTEATLASPUBL|INTNOTEATLASINT|InternalNote',
                            str(collection['primary']))
# --------------------------------------------------------------------------------------------------------------------
# AUTHORS PROCESSING
# --------------------------------------------------------------------------------------------------------------------

# verifying author's uniqueness in Virtuoso
# Step 1: search in Virtuoso authors with the same INSPIRE Number (if find - add theirs GUIDs to JSON)
# Step 2: ... with the same Control Number
# Step 3: ... with the same e-mails
# Step 4: ... with the same combination of first_name, last_name and affiliation
# Step 5: ... with the same combination of first_name and last_name
#
# Steps [1-4] allows to assume that the author is already exist in the database
# Step 5 is insufficient to confirm the author's overlap, so the new author GUID is generating

def authors_processing(data):
    authors = data['authors']
    print len(data['authors'])
    emails = data['email_message']
    for item in authors:
        item['potential_guids'] = []
        item['isNew'] = False
        item['type'] = 'Person'

    diff = authors

    for item in author_params_list:
        if (len(diff) > 0):
            if type(item['CDS']) is str:
                params_list = extraction(item['CDS'], diff, emails)
                print item['CDS'] + " length = " + str(len(params_list))
                if (len(params_list) > 0):

                    results = getAuthorsByParameter(item['SPARQL'], item['ONTO'], list2string(params_list))
                    print "Found in database: " + str(len(results))
                    if (item['CDS'] == 'control_number'):
                        item['type'] = 'ATLASMember'
                    else:
                        item['type'] = 'Person'
                    processing_request_results(results, authors, item['CDS'], item['SPARQL'])

            elif type(item['CDS'] is set):
                print len(diff)
                for d in diff:
                    if 'affiliation' in d:
                        affiliation = fixString(str(d['affiliation']))
                    else:
                        affiliation = ''
                    results = getAuthorsByFLA(fixString(d['first_name']), fixString(d['last_name']),
                                              fixString(d['first_name'][:1]), affiliation)
                    for res in results:
                        guid = res['guid']['value']
                        if 'affiliation' in res and res['affiliation'] != '':
                           # affiliation = res['affiliation']['value']
                            for author in authors:
                                if (author['first_name'] == d['first_name'] and
                                    author['last_name'] == d['last_name'] and
                                    author['affiliation'] == d['affiliation']) :
                                    if 'guid' in author:
                                        author['potential_guids'].append(guid)
                                    else:
                                        author['guid'] = guid
                                        # author['isNew'] = True
                        else:
                            for author in authors:
                                if (author['first_name'] == d['first_name'] and
                                            author['last_name'] == d['last_name']):
                                    if 'guid' not in author:
                                        author['guid'] = generateNewGUID(ontology_params['person_resource'])
                                        author['isNew'] = True
                                        print "Found new author:" + str(author)
            # get all elements which don't have GUIDs
            diff = diff_without_GUID(authors)
            print "DIFF LENGTH = " + str(len(diff))
    if len(diff) > 0:
        for author in diff:
            author['guid'] = generateNewGUID(ontology_params['person_resource'])
            author['isNew'] = True
            print "Found new author:" + str(author)
    # print len(authors)
    return authors

# updating array of authors with GUID and Potential GUIDs
def processing_request_results(results, data, CDS, sparql_param):
    for res in results:
        guid = res['guid']['value']
        if (type(sparql_param) is str):
            param_value = res[sparql_param]['value']
            index = find_index(data, CDS, param_value)
            if 'guid' in data[index]:
                data[index]['potential_guids'].append(guid)
            else:
                data[index]['guid'] = guid

# generate TTL string for adding new author
def newAuthorTTL(author, docGUID):
    author = fixDictValues(author, ['first_name', 'last_name', 'affiliation', 'e-mail', 'INSPIRE_Number', 'control_number'])
    guid = author['guid']
    type = author['type']
    ttl = '<{guid}> a <{ontology}#{type}> .\n'.format(guid=guid, type=type, **ontology_params)
    for key in author.keys():
        idx = -1
        try:
            idx = find_index(author_attrs, 'CDS', key)
            curr_value = author[author_attrs[idx]['CDS']]
            ttl += '<{guid}> <{ontology}#{ONTO}> "{SPARQL}" .\n'.format(guid=guid, ONTO=author_attrs[idx]['ONTO'],
                                                                        SPARQL=curr_value, **ontology_params)
        except:
            continue
    if len(author['potential_guids']) > 0:
        for g in author['potential_guids']:
            ttl += '<{guid}> <{ontology}#hasSimilar> "{potential_guid}" .\n'.format(guid=guid,
                                                                                    potential_guid=g, **ontology_params)
    ttl += author2paperTTL(guid, docGUID)
    return ttl

# generate TTL for linking author with document
def author2paperTTL(authorGUID, docGUID):
    return '<{docGUID}> <{ontology}#hasAuthor> <{authorGUID}> .\n'.format(docGUID=docGUID,
                                                                          authorGUID=authorGUID, **ontology_params)

# generate all author's TTL string
def authorTTL(authors, docGUID):
    ttl = ''
    for item in authors:
        if item["isNew"] == True:
            ttl += newAuthorTTL(item, docGUID)
        else:
            ttl += author2paperTTL(item['guid'], docGUID)
    #print ttl
    return ttl


#--------------------------------------------------------------------------------------------------------------------
# CONVERTING JSON TO TTL
#--------------------------------------------------------------------------------------------------------------------
# read JSON document with supporting document metadata and generating TTL
def json2TTL(glance_id, data):
    # search documentGUID in VIRTUOSO
    # print data
    ttl = ''
    docGUID = getDocumentGUID(glance_id)
    if docGUID is not None:
        docGUID = docGUID
    else:
        docGUID = generateNewGUID(ontology_params['document_resource'])
        ttl += '<{docGUID}> a <{ontology}#SupportingDocument> .\n'.format(docGUID=docGUID,
                                                                      **ontology_params)
        ttl += '<{docGUID}> <{ontology}#hasGLANCE_ID> {value} .\n'.format(docGUID=docGUID, value=glance_id,
                                                                      **ontology_params)
    report_numbers_processing(data['report_number'], data)
    if 'abstract' in data:              data['abstract']    = fixString(data['abstract']['summary'])
    if 'title' in data:                 data['title']       = fixString(data['title']['title'])
    if 'recid' in data:                 data['CDS_ID']      = data['recid']
    if 'creation_date' in data:         data['creation_date'] = fixString(data['creation_date'])
    for item in paper_attrs:
        curr_value = data[item['CDS']]
        if type(curr_value) is str:
            ttl += '<{docGUID}> <{ontology}#{ONTO}> "{value}" .\n'.format(docGUID=docGUID,
                                                                        ONTO=item['ONTO'], value=curr_value,
                                                                          **ontology_params)
        elif type(curr_value) is int:
            ttl += '<{docGUID}> <{ontology}#{ONTO}> {value} .\n'.format(docGUID=docGUID,
                                                                        ONTO=item['ONTO'], value=curr_value,
                                                                        **ontology_params)
    if 'keywords' in data:          ttl += keywordsTTL(data['keywords'], docGUID)
    # if 'authors' in data:           ttl += authorTTL(data['authors'], docGUID)
    return ttl

def keywordsTTL(keywords, docGUID):
    ttl = ''
    kw = []
    if type(keywords) is list:
        kw = [item['term'] for item in list(keywords)]
    elif type(keywords) is str:
        splitted = keywords.get('term').split(',')
        if len(splitted) > 0:
            kw = splitted
        else:
            kw.append(keywords.get('term'))
    elif type(keywords) is dict:
        kw.append(keywords.get('term'))
    for item in fixListValues(kw):
        ttl += _ttl_keyword.format(docGUID=docGUID, keyword=item, **ontology_params)
    return ttl

def report_numbers_processing(report_number, data):
    if type(report_number) is list:
        for item in report_number:
            if ('internal' in item):      CDSInternal = item['internal']
            if ('report_number' in item): CDS_ReportNumber = item['report_number']
    elif type(report_number) is dict:
        if 'internal' in report_number:
            CDSInternal = report_number['internal']
        elif 'internal' not in report_number:
            if 'primary_report_number' in report_number:
                CDSInternal = report_number['primary_report_number']
        if 'report_number' in report_number:
            CDS_ReportNumber = report_number['report_number']
    data['CDSInternal'] = CDSInternal
    #data['CDS_ReportNumber'] = CDS_ReportNumber


# -------------------------------------------------------------------------------------------------------------------
# HELPER FUNCTIONS
#--------------------------------------------------------------------------------------------------------------------
# fix escape sequences in strings
def fixString(s):
    return s.encode('ascii', 'ignore').replace("'", "\\'").replace("\n", "\\n").replace("\\", r"\\").replace('\"', '')

def fixDictValues(dict, keys_to_fix):
    for key in keys_to_fix:
        if key in dict.keys():
            if type(dict[key]) in (str, unicode):
                dict[key] = fixString(dict[key])
            elif type(dict[key]) is list:
                dict[key] = fixString(str(dict[key]))
            #print dict[key]
        else:
            continue
    return dict

def fixListValues(list):
    for item in list:
        item = fixString(item)
    return list

def list2string(list):
    str = '", "'.join(list)
    return '"' + str + '"'

def extraction(param, data_array, emails):
    new_arr = []
    if param == 'INSPIRE_Number':
        if 'control_number' in data_array:
            for ids in data_array['control_number']:
                if re.search("INSPIRE-\d+", ids):
                    new_arr.append(ids)
        return new_arr
    elif param == 'control_number':
        if 'control_number' in data_array:
            for ids in data_array['control_number']:
                if re.search("\\(\w+\\)\d+", ids):
                    new_arr.append(ids)
        return new_arr
    elif param == 'e-mail':
        if 'e-mail' in data_array:
            for ids in data_array['e-mail']:
                if re.search("\\(\w+\\)\d+", ids):
                    new_arr.append(ids)
        if emails:
            for e in emails:
                new_arr.append(e['address'])
    else:
        return [d[param] for d in data_array if param in d]

def diff_without_GUID(data):
    return [i for i in data if 'guid' not in i]

def generateNewGUID(resource):
    return resource + str(uuid.uuid4())

def find_index(dicts, key, value):
    class Null: pass
    for i, d in enumerate(dicts):
        if d.get(key, Null) == value:
            return i
    else:
        raise ValueError('no dict with the key and value combination found')

#  convert mixed data (list and dicts) to list representation
def convert_to_list(data):
    list_dicts = []
    if type(data) is dict:
        list_dicts.append(fixDictValues(data, ['first_name', 'last_name', 'affiliation', 'e-mail', 'INSPIRE_Number', 'control_number']))
    elif type(data) is list:
        for item in data:
            list_dicts.append(fixDictValues(item, ['first_name', 'last_name', 'affiliation', 'e-mail', 'INSPIRE_Number', 'control_number']))
    return list_dicts

# fix escape symbols in JSON strings
def fix_JSON(json_message=None):
    result = None
    try:
        result = json.loads(json_message)
    except Exception as e:
        # Find the offending character index:
        idx_to_replace = int(e.message.split(' ')[-1].replace(')', ''))
        # Remove the offending character:
        json_message = list(json_message)
        json_message[idx_to_replace] = ' '
        new_message = ''.join(json_message)
        return fix_JSON(json_message=new_message)
    return result

# TODO:
# Current tests showed no files with improper content
# So, this function is here just for test if these files will exist
# usage:
#             # jsons = fix_improper_JSON_content(content)
            # for item in jsons:
            #     json_processing(json.loads(item), output)
def fix_improper_JSON_content(content):
    jsons = []
    if content.find('][') != -1:
        print "Founded improper content"
        jsons = content.split('][')
        jsons[0] = str(jsons[0]) + ']'
        jsons[-1] = '[' + str(jsons[-1])
        for i in range(1, len(jsons) - 2):
            jsons[i] = '[' + str(jsons[i]) + ']'
    else:
        jsons.append(content)
    return jsons


if __name__ == '__main__':
  main(sys.argv[1:])
