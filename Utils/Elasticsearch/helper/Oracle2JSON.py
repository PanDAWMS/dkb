import json
import argparse
import ConfigParser
import DButils
import time
import re
import os

def main():
    """
    --input <SQL file> --output <directory> --size 1000
    :return:
    """
    args = parsingArguments()
    if (args.input):
        global INPUT
        INPUT = args.input
    if (args.output):
        global OUTPUT
        OUTPUT = args.output
    global SIZE
    if (args.size):
        SIZE = args.size
    else:
        SIZE = 500
    Config = ConfigParser.ConfigParser()
    Config.read("../settings.cfg")
    global dsn
    dsn = Config.get("oracle", "dsn")

    start = time.time()

    oracle2json(INPUT, OUTPUT, SIZE)
    end = time.time()
    print(end - start)

def oracle2json(sql_file, output, arraysize=500):
    """
    Processing query row by row, with parsing LOB values.
    :param sql_file: file with SQL query
    :param output: output directory
    :param arraysize: number of rows, processed at a time
    :return:
    """
    conn, cursor = DButils.connectDEFT_DSN(dsn)
    sql_handler = open(sql_file)
    result = DButils.OneByOneIter(conn, sql_handler.read()[:-1], True)
    counter = -1
    if not os.path.exists(output):
        os.makedirs(output)
    json_handler = open('%s/%s_%d.json' % (output, output, 0), 'wb')
    result_arr = []

    for i in result:
        i["phys_category"] = get_category(i.get("hashtag_list"), i.get("taskname"))
        json_body = json.dumps(i, ensure_ascii=False)
        if (counter%int(arraysize) == 0):
            json_handler = open('%s/%s_%d.json' % (output, output, counter), 'wb')
            json_handler.write('[')
        result_arr.append(json_body)
        json_handler.write(json_body)
        counter += 1
        if (counter % int(arraysize) != 0):
            json_handler.write(',')
            json_handler.write('\n')
        else:
            json_handler.write(']')
            json_handler.close()
    if not json_handler.closed:
        json_handler.seek(-1, os.SEEK_END)
        json_handler.truncate()
        json_handler.write(']')
        json_handler.close()

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

if  __name__ =='__main__':
    main()

