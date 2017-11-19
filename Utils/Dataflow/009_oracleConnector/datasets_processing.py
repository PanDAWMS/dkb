#!/bin/env python
import rucio.client
import argparse
import json
import re
import sys

DATASET_TYPE = ['input', 'output']
rucio_client = rucio.client.Client()

def main():
    args = parsingArguments()
    global INPUT
    INPUT = args.input
    process_data()

def process_data():
    input_file = open(INPUT)
    for line in input_file:
        json_str = json.loads(line)
        ds = {}
        ds['taskid'] = json_str.get('taskid')
        for type in DATASET_TYPE:
            datasets_to_array(json_str, type, ds)
        sys.stdout.write(json.dumps(ds) + '\n')

def datasets_to_array(data, type, ds):
    ds[type] = []
    if data.get(type) is not None:
        for dataset in data.get(type):
            ds_dict = {}
            ds_dict['datasetname'] = dataset
            try:
                bytes = get_metadata_attribute(rucio_client, remove_tid(dataset), 'bytes')
                if bytes == 'null' or bytes is None:
                    ds_dict['bytes'] = ''
                else:
                    ds_dict['bytes'] = bytes
                ds[type].append(ds_dict)
            except:
                ds_dict['bytes'] = ''
                ds[type].append(ds_dict)
    return ds

def extract_scope(dsn):
        if dsn.find(':') > -1:
            return dsn.split(':')[0], dsn.split(':')[1]
        else:
            scope = dsn.split('.')[0]
            if dsn.startswith('user') or dsn.startswith('group'):
                scope = '.'.join(dsn.split('.')[0:2])
            return scope, dsn

def remove_tid(dsn):
    return re.sub('(_tid[0-9]+_[0-9]+)', '', dsn)

def get_metadata_attribute(rucio_client, dsn, attribute_name):
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