'''
This script executed from lxplus.cern.ch
commands before executing script
- voms-proxy-init -voms atlas
- source /afs/cern.ch/atlas/software/tools/pyAMI/setup.sh
- export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
- source $ATLAS_LOCAL_ROOT_BASE/user/atlasLocalSetup.sh
- localSetupPyAMI

Get All periods for the defined period and for levels (1,2,3)
'''
import os
import json
import re
import pyAMI.client
import pyAMI.atlas.api as AtlasAPI
import json
from collections import OrderedDict

year_start = 2010
year_end   = 2017

client = pyAMI.client.Client('atlas')

AtlasAPI.init()

periods_file = "periods.json"

f = open(periods_file, 'w')
tmp = []
for year in range(year_start, year_end):
    for level in range(1,4):
        try:
            result =  AtlasAPI.list_dataperiods(client, level=level, year=year)
            tmp.append(",".join(str(json.dumps(item, indent=4)) for item in result))
        except:
            print "No periods found!"
f.write("[" + ",".join(str(x) for x in tmp) + "]")
f.close()