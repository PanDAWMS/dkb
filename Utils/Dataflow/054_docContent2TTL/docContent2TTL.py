#!/usr/bin/env python

"""
Stage 054: document content metadata to TTL
"""

import sys
sys.path.append("../")

import pyDKB
from pyDKB.dataflow import messages

graph = "http://nosql.tpu.ru:8890/DAV/ATLAS"
ontology = "http://nosql.tpu.ru/ontology/ATLAS"


def process(stage, msg):
    """Handling input data (triples) from doc_content_triples()
    Input message: JSON
    Output message: TTL
    """
    input_data = msg.content()
    result = doc_content_triples(input_data)
    if not result:
        return False

    for i in result:
        stage.output(pyDKB.dataflow.messages.TTLMessage(i))
    return True


def doc_content_triples(data):
    """Forming data into triples"""
    dataset_suffix = "_datasets"
    try:
        dkbID = data['dkbID']
    except KeyError:
        sys.stderr.write("No dkbID.\n")
        return False
    ttl = []
    for i, item in enumerate(data['content']):
        DEFAULT = {
            'graph': graph,
            'ontology': ontology,
            'c_name': item,
            'doc_ID': dkbID
        }
        if item.endswith(dataset_suffix):
            ttl.append("<{graph}/document/{doc_ID}/{c_name}>" \
                       " a <{ontology}#DocumentContent> ."
                       .format(**DEFAULT)
                       )
            ttl.append("<{graph}/document/{doc_ID}>" \
                       " <{ontology}#hasContent>" \
                       " <{graph}/document/{doc_ID}/{c_name}> ."
                       .format(**DEFAULT)
                       )
            for j, ds_item in enumerate(data['content'][item]):
                DATASAMPLES = {
                    'data_sample': ds_item
                }
                DATASAMPLES.update(DEFAULT)
                ttl.append("<{graph}/datasample/{data_sample}>" \
                           " a <{ontology}#DataSample> ."
                           .format(**DATASAMPLES)
                           )
                ttl.append("<{graph}/document/{doc_ID}/{c_name}>" \
                           " <{ontology}#mentionsDataSample>" \
                           " <{graph}/datasample/{data_sample}> ."
                           .format(**DATASAMPLES)
                           )
        if item == 'plain_text':
            try:
                data_taking_year = data['content'][item]['data taking year']
            except KeyError:
                data_taking_year = None
            try:
                luminosity = data['content'][item]['luminosity'] \
                    .replace(' ', '_')
            except (KeyError, AttributeError):
                luminosity = None
            try:
                energy = data['content'][item]['energy'] \
                    .lower().replace(' ', '')
            except (KeyError, AttributeError):
                energy = None
            PLAINTEXT = {
                'data_taking_year': data_taking_year,
                'luminosity': luminosity,
                'energy': energy
            }
            PLAINTEXT.update(DEFAULT)
            ttl.append("<{graph}/document/{doc_ID}/{c_name}>" \
                       " a <{ontology}#DocumentContent> ."
                       .format(**PLAINTEXT)
                       )
            ttl.append("<{graph}/document/{doc_ID}>" \
                       " <{ontology}#hasContent>" \
                       " <{graph}/document/{doc_ID}/{c_name}> ."
                       .format(**PLAINTEXT)
                       )
            if data_taking_year:
                ttl.append("<{graph}/document/{doc_ID}/{c_name}>" \
                           " <{ontology}#mentionsDataTakingYear>" \
                           " \"{data_taking_year}\" ."
                           .format(**PLAINTEXT)
                           )
            if luminosity:
                ttl.append("<{graph}/luminosity/{luminosity}>" \
                           " a <{ontology}#Luminosity> ."
                           .format(**PLAINTEXT)
                           )
                ttl.append("<{graph}/document/{doc_ID}/{c_name}>" \
                           " <{ontology}#mentionsLuminosity>" \
                           " <{graph}/luminosity/{luminosity}> ."
                           .format(**PLAINTEXT)
                           )
            if energy:
                ttl.append("<{graph}/document/{doc_ID}/{c_name}>" \
                           " <{ontology}#mentionsEnergy>" \
                           " <{ontology}#{energy}> ."
                           .format(**PLAINTEXT)
                           )
            try:
                campaign = data['content'][item]['campaigns']
                for j, pt_item in enumerate(campaign):
                    PLAINTEXT["campaign"] = pt_item
                    ttl.append("<{graph}/campaign/{campaign}>" \
                               " a <{ontology}#Campaign> ."
                               .format(**PLAINTEXT)
                               )
                    ttl.append("<{graph}/document/{doc_ID}/{c_name}>" \
                               " <{ontology}#mentionsCampaign>" \
                               " <{graph}/campaign/{campaign}> ."
                               .format(**PLAINTEXT)
                               )
                    del PLAINTEXT["campaign"]
            except KeyError:
                campaign = []
            if not campaign:
                sys.stderr.write("No campaigns in this file.\n")
    return ttl


def main(args):
    """Parsing command line arguments and processing JSON
    string from file or from stream
    """
    stage = pyDKB.dataflow.stage.JSON2TTLProcessorStage()
    stage.process = process

    exit_code = 0
    try:
        stage.parse_args(args)
        stage.run()
    except (pyDKB.dataflow.DataflowException, RuntimeError), err:
        if str(err):
            sys.stderr.write("(ERROR) %s\n" % err)
        exit_code = 1
    finally:
        stage.stop()

    exit(exit_code)


if __name__ == '__main__':
    main(sys.argv[1:])
