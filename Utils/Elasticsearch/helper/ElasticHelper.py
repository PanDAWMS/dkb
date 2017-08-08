from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException
import pprint
import json
import argparse
import ConfigParser
import datetime
import DButils
import time
import re

def main():
    """
    --host <...> --port 9200 --user <...> --pwd <...>
    :return:
    """
    args = parsingArguments()
    if (args.host):
        global HOST
        HOST = args.host
    if (args.port):
        global PORT
        PORT = args.port
    if (args.user and args.pwd):
        global USER
        USER = args.user
        global PWD
        PWD = args.pwd
    if (args.user and args.pwd):
        es = Elasticsearch([HOST+':'+PORT], http_auth=(USER, PWD))
    else:
        es = Elasticsearch([HOST+':'+PORT])

    Config = ConfigParser.ConfigParser()
    Config.read("settings.cfg")
    global dsn
    dsn = Config.get("oracle", "dsn")
    start = time.time()

    indexing(es_instance=es,
             index_name='mc16',
             doc_type='event_summary',
             sql_file='../../OracleProdSys2/mc16_campaign_for_ES.sql',
             remove_old=True,
             mapping_file=None,
             keyfield='taskid')

    end = time.time()
    print(end - start)


def indexing(es_instance, index_name, doc_type, sql_file, remove_old=True, mapping_file=None, keyfield=None):
    """
    Procedure, which executes SQL Request,
    process it iteratively and put data into ElasticSearch index.

    :param es_instance: Current instance of ElasticSearch
    :param index_name: Name of index
    :param doc_type: Document Type
    :param sql_file: SQL File with Request
    :param remove_old: Remove index from ElasticSearch? True = Yes, by default
    :param mapping_file: Put mapping file (Auto-mapping by default)
    :param keyfield: Define field, which could be used for IDs
    :return:
    """
    # remove old index
    if remove_old:
        removeIndex(index_name, es_instance)
        # recreate index
        if mapping_file is not None:
            if isinstance(mapping_file, str):
                handler = open(mapping_file)
                mapping = handler.read()
                es_instance.indices.create(index=index_name,
                                           body=mapping)
        else:
            es_instance.indices.create(index=index_name)
    conn, cursor = DButils.connectDEFT_DSN(dsn)
    handler = open(sql_file)
    result = DButils.ResultIter(conn, handler.read()[:-1], 100, True)

    # set current timestamp
    curr_tstamp = datetime.datetime.now()
    id_counter = 0
    for i in result:
        i["phys_category"] = get_category(i.get("hashtag_list"), i.get("taskname"))
        json_body = json.dumps(i, ensure_ascii=False)
        try:
            res = es_instance.index(index=index_name,
                           doc_type=doc_type,
                           id=i[keyfield] if keyfield is not None else id_counter+1,
                           body=json_body,
                           timestamp=curr_tstamp)
            pprint.pprint(res)
        except ElasticsearchException as e:
            print json_body
            print e

def get_category(hashtags, taskname):
    PHYS_CATEGORIES_MAP = {'BPhysics':['charmonium','jpsi','bs','bd','bminus','bplus','charm','bottom','bottomonium','b0'],
                            'BTag':['btagging'],
                            'Diboson':['diboson','zz', 'ww', 'wz', 'wwbb', 'wwll'],
                            'DrellYan':['drellyan', 'dy'],
                            'Exotic':['exotic', 'monojet', 'blackhole', 'technicolor', 'randallsundrum',
                            'wprime', 'zprime', 'magneticmonopole', 'extradimensions', 'warpeded',
                            'randallsundrum', 'contactinteraction','seesaw'],
                            'GammaJets':['photon', 'diphoton'],
                            'Higgs':['whiggs', 'zhiggs', 'mh125', 'higgs', 'vbf', 'smhiggs', 'bsmhiggs', 'chargedhiggs'],
                            'Minbias':['minbias'],
                            'Multijet':['dijet', 'multijet', 'qcd'],
                            'Performance':['performance'],
                            'SingleParticle':['singleparticle'],
                            'SingleTop':['singletop'],
                            'SUSY':['bino', 'susy', 'pmssm', 'leptosusy', 'rpv','mssm'],
                            'Triboson':['triplegaugecoupling', 'triboson', 'zzw', 'www'],
                            'TTbar':['ttbar'],
                            'TTbarX':['ttw','ttz','ttv','ttvv','4top','ttww'],
                            'Upgrade':['upgrad'],
                            'Wjets':['w'],
                            'Zjets':['z']}
    match = {}
    categories = []
    for phys_category in PHYS_CATEGORIES_MAP:
        current_map = [x.strip(' ').lower() for x in PHYS_CATEGORIES_MAP[phys_category]]
        match[phys_category] = len([x for x in hashtags.lower().split(',') if x.strip(' ') in current_map])
    categories = [cat for cat in match if match[cat] > 0]
    if len(categories) > 0:
        return categories
    else:
        phys_short = taskname.split('.')[2].lower()
        if re.search('singletop', phys_short) is not None: categories.append("SingleTop")
        if re.search('ttbar', phys_short) is not None: categories.append("TTbar")
        if re.search('jets', phys_short) is not None: categories.append("Multijet")
        if re.search('h125', phys_short) is not None: categories.append("Higgs")
        if re.search('ttbb', phys_short) is not None: categories.append("TTbarX")
        if re.search('ttgamma', phys_short) is not None: categories.append("TTbarX")
        if re.search('_tt_', phys_short) is not None: categories.append("TTbar")
        if re.search('upsilon', phys_short) is not None: categories.append("BPhysics")
        if re.search('tanb', phys_short) is not None: categories.append("SUSY")
        if re.search('4topci', phys_short) is not None: categories.append("Exotic")
        if re.search('xhh', phys_short) is not None: categories.append("Higgs")
        if re.search('3top', phys_short) is not None: categories.append("TTbarX")
        if re.search('_wt', phys_short) is not None: categories.append("SingleTop")
        if re.search('_wwbb', phys_short) is not None: categories.append("SingleTop")
        if re.search('_wenu_', phys_short) is not None: categories.append("Wjets")
        return categories
    return "Uncategorized"

def parsingArguments():
    parser = argparse.ArgumentParser(description='Process command line arguments.')
    parser.add_argument('--host', help='ElasticSearch host')
    parser.add_argument('--port', help='ElasticSearch port')
    parser.add_argument('--user', help='ElasticSearch user')
    parser.add_argument('--pwd', help='ElasticSearch password')
    return parser.parse_args()


def removeIndex(index, es):
    try:
        res = es.indices.delete(index=index, ignore=[400, 404])
        print('Index ' + index + 'has beend removed from ElasticSearch')
    except:
        print('Index could not be deleted')

