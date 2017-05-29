#!/usr/bin/env python
"""
Module document2ttl.py
- input from step 015 JSON:
            {
              "GLANCE": {},
              "CDS" : {},
              "dkbID" : ...,
              "supporting_notes": [
                    {
                        "GLANCE": {},
                        "CDS": {},
                        "dkbID": ...,
                    },
                    {
                        ...
                    }
              ]
            }
- output to TTL format
            PAPER a atlas:Paper .
            PAPER atlas:hasGLANCE_ID __ .
            PAPER atlas:hasShortTitle __ .
            PAPER atlas:hasFullTitle __ .
            PAPER atlas:hasRefCode __ .
            PAPER atlas:hasCreationDate __ .
            PAPER atlas:hasCDSReportNumber __ .
            PAPER atlas:hasCDSInternal __ .
            PAPER atlas:hasCDS_ID __ .
            PAPER atlas:hasAbstract __ .
            PAPER atlas:hasArXivCode __ .
            PAPER atlas:hasFullTitle __ .
            PAPER atlas:hasDOI __ .
            PAPER atlas:hasKeyword __ .
            JOURNAL_ISSUE a atlas:JournalIssue .
            JOURNAL_ISSUE atlas:hasTitle __ .
            JOURNAL_ISSUE atlas:hasVolume __ .
            JOURNAL_ISSUE atlas:hasYear __ .
            JOURNAL_ISSUE atlas:containsPublication> PAPER .
            SUPPORTING_DOCUMENT a atlas:SupportingDocument .
            SUPPORTING_DOCUMENT atlas:hasGLANCE_ID __ .
            SUPPORTING_DOCUMENT atlas:hasLabel __ .
            SUPPORTING_DOCUMENT atlas:hasURL __ .
            SUPPORTING_DOCUMENT atlas:hasCreationDate __ .
            SUPPORTING_DOCUMENT atlas:hasCDSInternal __ .
            SUPPORTING_DOCUMENT atlas:hasCDS_ID __ .
            SUPPORTING_DOCUMENT atlas:hasAbstract __ .
            SUPPORTING_DOCUMENT atlas:hasKeyword __ .
            PAPER atlas:isBasedOn SUPPORTING_DOCUMENT .

TODO: This module doesn't convert authors metadata. This task is still under consideration.
"""
import argparse
import sys
import json
import urllib
import urllib2
sys.path.append("../")
from pyDKB.dataflow import stage

#defaults
GRAPH = "http://nosql.tpu.ru:8890/DAV/ATLAS"
ONTOLOGY = "http://nosql.tpu.ru/ontology/ATLAS"
SPARQL = "http://nosql.tpu.ru:8890/sparql"

# Lists of dictionaries with parameters names for JSON documents and Ontology representation

PAPER_GLANCE_ATTRS = [{'GLANCE': 'id', 'ONTO': 'hasGLANCE_ID', 'TYPE': '^^xsd:int'},
                      {'GLANCE': 'short_title', 'ONTO': 'hasShortTitle', 'TYPE': ''},
                      {'GLANCE': 'full_title', 'ONTO': 'hasFullTitle', 'TYPE': ''},
                      {'GLANCE': 'ref_code', 'ONTO': 'hasRefCode', 'TYPE': ''},]

NOTE_GLANCE_ATTRS = [{'GLANCE': 'id', 'ONTO': 'hasGLANCE_ID', 'TYPE': '^^xsd:int'},
                     {'GLANCE': 'label', 'ONTO': 'hasLabel', 'TYPE': ''},
                     {'GLANCE': 'url', 'ONTO': 'hasURL', 'TYPE': ''},]

PAPER_CDS_ATTRS = [{'CDS': 'creation_date', 'ONTO': 'hasCreationDate', 'TYPE': '^^xsd:dateTime'},
                   {'CDS': 'CDS_ReportNumber', 'ONTO': 'hasCDSReportNumber', 'TYPE' : ''},
                   {'CDS': 'CDSInternal', 'ONTO': 'hasCDSInternal', 'TYPE': ''},
                   {'CDS': 'CDS_ID', 'ONTO': 'hasCDS_ID', 'TYPE': '^^xsd:integer'},
                   {'CDS': 'abstract', 'ONTO': 'hasAbstract', 'TYPE': ''},
                   {'CDS': 'primary_report_number', 'ONTO': 'hasArXivCode', 'TYPE': ''},
                   {'CDS': 'title', 'ONTO': 'hasFullTitle', 'TYPE': ''},]


NOTE_CDS_ATTRS = [{'CDS': 'creation_date', 'ONTO': 'hasCreationDate', 'TYPE': '^^xsd:dateTime'},
                  {'CDS': 'CDSInternal', 'ONTO': 'hasCDSInternal', 'TYPE': ''},
                  {'CDS': 'CDS_ID', 'ONTO': 'hasCDS_ID', 'TYPE': '^^xsd:integer'},
                  {'CDS': 'abstract', 'ONTO': 'hasAbstract', 'TYPE': ''},
                  {'CDS': 'title', 'ONTO': 'hasFullTitle', 'TYPE': ''},]

# SPARQL Queries

SPARQL_QUERY = '''
                WITH <{graph}> SELECT ?guid, ?{param_name}
                WHERE {{
                    ?guid <{ontology}#{ONTO}> ?{param_name} .
                    FILTER(?{param_name} IN ({params_list}))
                }}
            '''

def define_globals(args):
    global GRAPH
    GRAPH = args.GRAPH

    global ONTOLOGY
    ONTOLOGY = args.ONTOLOGY

    global SPARQL
    SPARQL = args.SPARQL

def get_document_iri(doc_id):
    """
    :param doc_id:
    :return Document IRI for current graph:
    """
    obj = "document/%s" % doc_id
    return "<%s/%s>" % (GRAPH, obj)

def document_glance(data, doc_iri, glance_attrs):
    """
    converting document GLANCE metadata from JSON to TTL (Turtle)
    :param data: JSON data from file or stream
    :param doc_iri: document IRI
    :param glance_attrs: PAPER_GLANCE_ATTRS | NOTE_GLANCE_ATTRS
    :return ttl string with GLANCE metadata:
    """
    # if isinstance(data, dict):
    #     raise ValueError("expected parameter of type %s, got %s\n" % (dict, type(data)))
    ttl = ""
    for param in glance_attrs:
        data[param.get('GLANCE')] = glance_parameter_extraction(param.get('GLANCE'), data)
    for item in glance_attrs:
        curr_value = data[item.get('GLANCE')]
        ttl += '{docIRI} <{ontology}#{ONTO}> "{value}"{xsdType} .\n' \
            .format(docIRI=doc_iri, ontology=ONTOLOGY, ONTO=item.get('ONTO'),
                    value=curr_value, xsdType=item.get('TYPE'))
    return ttl

def documents_links(data):
    """
    Convert documents links to TTL
    :param data: metadata fro JSON file or stream
    :return ttl: ttl string with links
    PAPER atlas:isBasedOn SUPPORTING_DOCUMENT .
    """
    ttl = ''
    paper_iri = get_document_iri(data.get('dkbID'))
    for item in data.get('supporting_notes'):
        note_iri = get_document_iri(item.get('dkbID'))
        ttl += '{paperIRI} <{ontology}#isBasedOn> {noteIRI} .\n'\
            .format(paperIRI=paper_iri, ontology=ONTOLOGY, noteIRI=note_iri)
    return ttl

def document_cds(data, doc_iri, cds_attrs):
    """
    Read JSON document with supporting document metadata and generating TTL
    :param data: metadata fro JSON file or stream
    :param doc_iri: document IRI for current graph
    :param cds_attrs: PAPER_CDS_ATTRS | NOTE_CDS_ATTRS
    :return ttl: string with metadata
    """
    ttl = ''
    for param in cds_attrs:
        data[param.get('CDS')] = cds_parameter_extraction(param.get('CDS'), data)
    for item in cds_attrs:
        curr_value = data[item.get('CDS')]
        if curr_value is not None:
            ttl += '{docIRI} <{ontology}#{ONTO}> "{value}"{xsdType} .\n' \
                .format(docIRI=doc_iri, ontology=ONTOLOGY, ONTO=item.get('ONTO'),
                        value=curr_value, xsdType=item.get('TYPE'))
    # processing multivalue parameters
    if 'doi' in data:
        ttl += doi2ttl(data.get('doi'), doc_iri)
    if 'keywords' in data:
        ttl += keywords2ttl(data.get('keywords'), doc_iri)
    if 'publication_info' in data:
        ttl += process_journals(data.get('publication_info'), doc_iri)
    sys.stderr.write("done!\n")
    return ttl

def doi2ttl(doi, doc_iri):
    """
    Converting DOI parameter to TTL
    :param doi: doi from JSON string
    :param doc_iri: document IRI for current graph
    :return ttl: ttl string with DOIs
    """
    ttl = ''
    dois = []
    if isinstance(doi, str) or isinstance(doi, unicode):
        dois.append(doi)
    elif isinstance(doi, list):
        dois = doi
    for item in fix_list_values(dois):
        ttl += '{docIRI} <{ontology}#hasDOI> "{doi}" .\n'\
            .format(docIRI=doc_iri, doi=item, ontology=ONTOLOGY)
    return ttl

def keywords2ttl(keywords, doc_iri):
    """
    Converting keywords from JSON string to TTL
    :param keywords: keywords parameters from JSON string
    :param doc_iri: document IRI for current graph
    :return ttl: ttl string with keywords
    """
    ttl = ''
    keyword = []
    if isinstance(keywords, list):
        keyword = [item.get('term') for item in list(keywords)]
    elif isinstance(keywords.get('term'), str):
        splitted = keywords.get('term').split(',')
        if len(splitted) > 0:
            keyword = splitted
        else:
            keyword.append(keywords.get('term'))
    elif isinstance(keywords, dict):
        keyword.append(keywords.get('term'))
    for item in fix_list_values(keyword):
        ttl += '{docIRI} <{ontology}#hasKeyword> "{keyword}" .\n'\
            .format(docIRI=doc_iri, keyword=item, ontology=ONTOLOGY)
    return ttl

def cds_internal_extraction(data):
    """
    Extracting cds internal report number parameter from JSON string
    :param data: JSON string
    :return report number:
    """
    if 'report_number' in data:
        report_number = data.get('report_number')
        if isinstance(report_number, list):
            for item in report_number:
                if 'internal' in item:
                    return item.get('internal')
        elif isinstance(report_number, dict):
            if 'internal' in report_number:
                return report_number.get('internal')
            elif 'internal' not in report_number:
                if 'primary_report_number' in report_number:
                    return report_number.get('primary_report_number')

def report_number_extraction(data):
    """
    Exracting report number from JSON string
    :param data:
    :return:
    """
    if 'report_number' in data:
        report_number = data.get('report_number')
        if isinstance(report_number, list):
            for item in report_number:
                if 'report_number' in item:
                    return item.get('report_number')
        elif isinstance(report_number, dict):
            if 'report_number' in report_number:
                return report_number.get('report_number')


def glance_parameter_extraction(param_name, json_data):
    """
    Extracting single value parameters from GLANCE json 
    :param param_name: 
    :param json_data: JSON with GLANCE metadata
    :return:
    """
    if param_name == 'id':
        return json_data['id']
    elif param_name == 'short_title':
        return fix_string(json_data.get('short_title'))
    elif param_name == 'full_title':
        return fix_string(json_data.get('full_title'))
    elif param_name == 'ref_code':
        return json_data.get('ref_code')
    elif param_name == 'label':
        return fix_string(json_data.get('label'))
    elif param_name == 'url':
        return fix_string(json_data.get('url'))

def cds_parameter_extraction(param_name, json_data):
    """
    Extracting parameters from json string with CDS parameters
    :param param_name: name of parameter, defined in *_CDS_ATTRS dict
    :param json_data: json string with CDS parameters
    :return: 
    """
    if param_name == 'abstract':
        return abstract_extraction(json_data)
    if param_name == 'title':
        return title_extraction(json_data)
    if param_name == 'CDS_ID':
        return cds_id_extraction(json_data)
    if param_name == 'creation_date':
        return creation_date_extraction(json_data)
    if param_name == 'primary_report_number':
        return arxiv_extraction(json_data)
    if param_name == 'CDSInternal':
        return cds_internal_extraction(json_data)
    if param_name == 'CDS_ReportNumber':
        return report_number_extraction(json_data)

def abstract_extraction(data):
    """
    Extracting abstract from json string
    :param data: json string
    :return: string with abstract
    """
    if 'abstract' in data:
        return fix_string(data.get('abstract').get('summary'))

def title_extraction(data):
    """
    Extracting title from json string
    :param data: json string
    :return: string with title
    """
    if 'title' in data:
        return fix_string(data.get('title').get('title'))

def cds_id_extraction(data):
    """
    Extracting CDS_ID from json string
    :param data: json string
    :return: string with CDS_ID
    """
    if 'recid' in data:
        return int(data.get('recid'))

def creation_date_extraction(data):
    """
    Extracting creation date from json string
    :param data: json string
    :return: string with date
    """
    if 'creation_date' in data:
        return fix_string(data.get('creation_date'))

def arxiv_extraction(data):
    """
    Extracting of arXiv from json string
    :param data: json string
    :return: string with arXiv
    """
    if 'primary_report_number' in data:
        return fix_string(data.get('primary_report_number'))

def generate_journal_id(journal_dict):
    """
    Generating journal issue ID based on title, volume and year
    :param journal_dict: dictionary with journal parameters
    :return: journal ID
    """
    journal_id = ''
    if 'title' in journal_dict:
        journal_id += journal_dict.get('title').replace(" ", "")
    if 'volume' in journal_dict:
        journal_id += '_' + journal_dict.get('volume').replace(" ", "")
    if 'year' in journal_dict:
        journal_id += '_' + journal_dict.get('year').replace(" ", "")
    return journal_id

def process_journals(data, doc_iri):
    """
    Convert journal data from json string to TTL
    :param data: json string
    :param doc_iri: document IRI for current graph
    :return: ttl string with journal issue with connection to paper
    """
    journals = convert_to_list(data)
    ttl = ''
    for item in journals:
        journal_id = generate_journal_id(item)
        if get_journal_by_id(journal_id):
            ttl += '<{journal_resource}{journalIssueID}> ' \
                   '<{ontology}#containsPublication> {doc_iri} .\n'\
                .format(journalIssueID=journal_id,
                        doc_iri=doc_iri,
                        ontology=ONTOLOGY,
                        journal_resource=GRAPH+'/journal_issue/')
        else:
            ttl += '''<{journal_resource}{journalIssueID}> a <{ontology}#JournalIssue> .
       <{journal_resource}{journalIssueID}> <{ontology}#hasTitle> "{title}"^^xsd:string .
       <{journal_resource}{journalIssueID}> <{ontology}#hasVolume> "{volume}"^^xsd:string .
       <{journal_resource}{journalIssueID}> <{ontology}#hasYear> "{year}"^^xsd:string .
       <{journal_resource}{journalIssueID}> <{ontology}#containsPublication> {doc_iri} .
       '''.format(journalIssueID=journal_id, title=item.get('title'), volume=item.get('volume'),
                  year=item.get('year'), doc_iri=doc_iri, journal_resource=GRAPH+'/journal_issue/',
                  ontology=ONTOLOGY)
    return ttl

def sparql_query(query, base_url, output_format="application/sparql-results+json"):
    """
    Execute SPARQL requests with urllib2 library
    :param query:
    :param base_url:
    :param output_format:
    :return:
    """
    params = {
        "query": query,
        "format": output_format
    }
    querypart = urllib.urlencode(params)
    try:
        response = urllib.urlopen(base_url, querypart).read()
    except urllib2.HTTPError as exception:
        sys.stderr.write('The server couldn\'t fulfill the request.')
        sys.stderr.write('Error code: '), exception.code
    except urllib2.URLError as exception:
        sys.stderr.write('We failed to reach a server.')
        sys.stderr.write('Reason: '), exception.reason
    else:
        return json.loads(response)


def has_results(results):
    """
    check if results in Virtuoso
    :param results:
    :return:
    """
    if isinstance(results.get('results').get('bindings'), list):
        length = len(results.get('results').get('bindings'))
    return True if length > 0 else False

def get_journal_by_id(journal_id):
    """
    Search journal ID in Virtuoso DB
    :param journal_id:
    :return:
    """
    journal_query = '''WITH <{graph}> SELECT count(?journal)
                    WHERE {{
                        <{journal_resource}{journalIssueID}> <{rdf_prefix}#type> ?journal .
                    }}'''.format(journalIssueID=journal_id, graph=GRAPH,
                                 journal_resource=GRAPH+'/journal_issue/',
                                 rdf_prefix='http://www.w3.org/1999/02/22-rdf-syntax-ns')
    results = sparql_query(journal_query, SPARQL)
    if has_results(results):
        res = results.get('results').get('bindings')[0]['callret-0']['value']
        return res != str('0')


def fix_string(wrong_string):
    """
    fix escape sequences in strings
    :param wrong_string:
    :return:
    """
    return wrong_string.encode('ascii', 'ignore').replace("'", "\\'")\
        .replace("\n", "\\n").replace("\\", r"\\").replace('\"', '')

def fix_list_values(list_vals):
    """
    Fixing list values with fix_string
    :param list_vals:
    :return:
    """
    for item in list_vals:
        item = fix_string(item)
    return list_vals

def fix_dict_values(dict_vals, keys_to_fix):
    """
    Fixing dictionary values with wrong unicode symbols
    :param dict_vals: initial dictionary
    :param keys_to_fix: list of keys to fix
    :return:
    """
    for key in keys_to_fix:
        if key in dict_vals.keys():
            if isinstance(dict_vals[key], str) or isinstance(dict_vals[key], unicode):
                dict_vals[key] = fix_string(dict_vals[key])
            elif isinstance(dict_vals[key], list):
                dict_vals[key] = fix_string(str(dict_vals[key]))
        else:
            continue
    return dict_vals

def write_ttl2file(output, ttl_string):
    """
    write ttl string with document metadata to TTL file
    :param output:
    :param ttl_string:
    :return:
    """
    try:
        ttl_file = open(output, "w+")
    except IOError:
        sys.stderr.write('cannot open file')
    else:
        try:
            ttl_file.write(ttl_string)
            ttl_file.write('\n\0')
        except IOError:
            sys.stderr.write('can\'t write to file')
        else:
            ttl_file.close()
            sys.stderr.write("TTL file has written!")

def convert_to_list(data):
    """
    convert mixed data (list and dicts) to list representation
    :param data:
    :return:
    """
    list_dicts = []
    if isinstance(data, dict):
        list_dicts.append(fix_dict_values(data, ['first_name',
                                                 'last_name',
                                                 'affiliation',
                                                 'e-mail',
                                                 'INSPIRE_Number',
                                                 'control_number']))
    elif isinstance(data, list):
        for item in data:
            list_dicts.append(fix_dict_values(item, ['first_name',
                                                     'last_name',
                                                     'affiliation',
                                                     'e-mail',
                                                     'INSPIRE_Number',
                                                     'control_number']))
    return list_dicts


def main(argv):
    """
    Parsing command line arguments and processing JSON string from file or from stream
    :param argv: arguments
    :return:
    """
    processor = stage.JSONProcessorStage()
    processor.add_argument('-g', '--graph', action='store', type=str, nargs='?',
                        help='Virtuoso DB graph name (default: %(default)s)',
                        default=GRAPH,
                        const=GRAPH,
                        metavar='GRAPH',
                        dest='GRAPH')
    processor.add_argument('-O', '--ontology', action='store', type=str, nargs='?',
                        help='Virtuoso ontology prefix (default: %(default)s)',
                        default=ONTOLOGY,
                        const=ONTOLOGY,
                        metavar='ONT',
                        dest='ONTOLOGY')
    processor.add_argument('-SPARQL', '--sparql', action='store', type=str, nargs='?',
                        help='SPARQL Endpoint (default: %(default)s)',
                        default=SPARQL,
                        const=SPARQL,
                        metavar='SPARQL',
                        dest='SPARQL')
    processor.add_argument('-delim', '--delimiter', action='store', nargs='?',
                        help=u'EOP marker for Kafka mode (default: \0)',
                        default='',
                        dest='EOPmarker')
    processor.parse_args(argv)
    define_globals(processor.ARGS)
    gen = processor.input()

    for f in gen:
        data = f.content()
        paper_id = data.get('dkbID')
        doc_iri = get_document_iri(paper_id)
        doc_ttl = ""
        doc_ttl += document_glance(data.get('GLANCE'), doc_iri, PAPER_GLANCE_ATTRS)
        doc_ttl += document_cds(data.get('CDS'), doc_iri, PAPER_CDS_ATTRS)

        # supporting documents processing

        if "supporting_notes" in data:
            for note in data.get('supporting_notes'):
                note_id = note.get('dkbID')
                note_iri = get_document_iri(note_id)
                doc_ttl += document_glance(note.get('GLANCE'), note_iri, NOTE_GLANCE_ATTRS)
                doc_ttl += document_cds(note.get('CDS'), note_iri, NOTE_CDS_ATTRS)

        doc_ttl += documents_links(data)

        if processor.ARGS.mode == 'f':
            if processor.ARGS.output_dir != '':
                output = processor.ARGS.output_dir + '/' + paper_id + '.ttl'
            else:
                output = paper_id + '.ttl'
            write_ttl2file(output, doc_ttl)
        else:
            sys.stdout.write(doc_ttl + processor.ARGS.EOPmarker)
            sys.stdout.flush()

if __name__ == "__main__":
    main(sys.argv[1:])
