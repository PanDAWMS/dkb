#!/bin/env python
import rucio.client
import argparse
import json
import re
import sys

rucio_client = rucio.client.Client()
DS_TYPE = 'output'

def main():
    args = parsingArguments()
    global INPUT
    INPUT = args.input
    process_data()

def process_data():
    input_file = open(INPUT)
    for line in input_file:
        # Construct NDJSON string with the following format:
        # { "taskid": <TASKID>,
        #   "output": []
        # }
        json_str = json.loads(line)
        ds = {}
        ds['taskid'] = json_str.get('taskid')
        datasets_to_array(json_str, ds)
        sys.stdout.write(json.dumps(ds) + '\n')

def datasets_to_array(data, ds):
    """
    Constructs the array of dictionaries with datasets with the following format:
    "output": [
        {"deleted": true | false,
         "datasetname": "<DS_NAME>",
         "bytes": <BYTES>}
    ]
    :param data: input JSON string
    :param ds: output dictionary
    :return:
    """
    ds[DS_TYPE] = []

    if data.get(DS_TYPE) is not None:
        for dataset in data.get(DS_TYPE):
            ds_dict = {}
            ds_dict['datasetname'] = dataset
            try:
                bytes = get_metadata_attribute(rucio_client, dataset, 'bytes')
                if bytes == 'null' or bytes is None:
                    ds_dict['bytes'] = -1
                else:
                    ds_dict['bytes'] = bytes
                    ds_dict['deleted'] = False
                ds[DS_TYPE].append(ds_dict)
            except:
                # if dataset wasn't find in Rucio, it means that it was deleted from
                # the Rucio catalog. In this case 'deleted' is set to TRUE and
                # the length of file is set to -1
                ds_dict['bytes'] = -1
                ds_dict['deleted'] = True
                ds[DS_TYPE].append(ds_dict)
    return ds

def extract_scope(dsn):
    """
    Extracs the first part of dataset name (ex: mc15_13TeV.XXX)
    :param dsn: full dataset name
    :return:
    """
    if dsn.find(':') > -1:
        return dsn.split(':')[0], dsn.split(':')[1]
    else:
        scope = dsn.split('.')[0]
        if dsn.startswith('user') or dsn.startswith('group'):
            scope = '.'.join(dsn.split('.')[0:2])
        return scope, dsn

def get_metadata_attribute(rucio_client, dsn, attribute_name):
    """
    Returns the value of the attribute from Rucio
    :param dsn: full dataset name
    :param attribute_name: name of searchable attribute
    :return:
    """
    scope, dataset = extract_scope(dsn)
    metadata = rucio_client.get_metadata(scope=scope, name=dataset)
    if attribute_name in metadata.keys():
        return metadata[attribute_name]
    else:
        return None

def parsingArguments():
    parser = argparse.ArgumentParser(description='Process command line arguments.')
    parser.add_argument('--input', help='Input file path',required=True)
    return parser.parse_args()

if  __name__ == '__main__':
    main()