import os
import json
import re
import pyAMI.client
import pyAMI.atlas.api as AtlasAPI
import json
from collections import OrderedDict

client = pyAMI.client.Client('atlas')

AtlasAPI.init()

runs_file = open("runs.txt", 'w')
periods_json = open("periods.json", 'r')

json_content = json.load(periods_json)

tmp = []

for item in json_content:
    period = []
    period.append(item['period'])
    level = item['periodLevel']
    projectName = item['projectName']
    year = str(re.findall(r'\d+', projectName)[0])
    try:
        result = AtlasAPI.list_runs(
            client, year=int(year), data_periods=period)
        tmp.append(",".join(str(json.dumps(item, indent=4))
                            for item in result))
    except pyAMI.exception.Error:
        print "Cannot find results for period = " + str(period)

runs_file.write("[" + ",".join(str(x) for x in tmp) + "]")
runs_file.close()
periods_json.close()
