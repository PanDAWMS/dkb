import json
import argparse
import ConfigParser
import time
import re
import os
import cx_Oracle
import sys

try:
    import cx_Oracle
except:
    print "****ERROR : DButils. Cannot import cx_Oracle"
    pass

def connectDEFT_DSN(dsn):
    connect = cx_Oracle.connect(dsn)
    cursor = connect.cursor()

    return connect, cursor

def main():
    """
    --input <SQL file>
    :return:
    """
    args = parsingArguments()
    if (args.input):
        global INPUT
        INPUT = args.input
    Config = ConfigParser.ConfigParser()
    Config.read("settings.cfg")
    global dsn
    dsn = Config.get("oracle", "dsn")

    start = time.time()

    oracle2json(INPUT)
    end = time.time()
    print(end - start)

def oracle2json(sql_file):
    """
    Processing query row by row, with parsing LOB values.
    :param sql_file: file with SQL query
    :param output: output directory
    :param arraysize: number of rows, processed at a time
    :return:
    """
    conn, cursor = connectDEFT_DSN(dsn)
    sql_handler = open(sql_file)
    cursor = conn.cursor()
    cursor.execute(sql_handler.read()[:-1])
    colnames = [i[0].lower() for i in cursor.description]
    row = cursor.fetchone()
    while row:
        row = cursor.fetchone()
        if not row:
            break
        row = fix_lob(row)
        sys.stdout.write(json.dumps(dict(zip(colnames, row)),ensure_ascii=False) + '\n')

def get_category(hashtags, taskname):
    """
    Each task can be associated with a number of Physics Categories.
    1) search category in hashtags list
    2) if not found in hashtags, then search category in phys_short field of tasknames
    :param hashtags: hashtag list from oracle request
    :param taskname: taskname
    :return:
    """
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
    parser.add_argument('--input', help='SQL file path')
    parser.add_argument('--output', help='Output directory')
    parser.add_argument('--size', help='Number of lines, processed at a time')
    return parser.parse_args()

def fix_lob(row):
    """
    This procedure is needed in case of using
    tables with LOB's values.
    AS usual LOB is JSON's.
    And we need to process JSON string as it was
    a set of columns.
    :param row:
    :return:
    """
    def convert(col):
        if isinstance(col, cx_Oracle.LOB):
            result = ''
            try:
                result = json.load(col)
            except:
                result = str(col)
            return result
        else:
            return col
    return [convert(c) for c in row]

if  __name__ =='__main__':
    main()

