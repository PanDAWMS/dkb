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
    if result == False:
        return False

    for i in result:
        stage.output(pyDKB.dataflow.messages.TTLMessage(i))
    return True

def doc_content_triples(data):
	listDatasets = ['montecarlo_datasets', 'group_datasets',
	'user_datasets', 'physcont_datasets', 'calibration_datasets', 'realdata_datasets', 'database_datasets']

	dkbID = data['dkbID']
	triples = []
	for i, item in enumerate(data['content']):
		#for any type of dataset
		if item in listDatasets:
			DATASETS = {
				'graph' : graph,
				'ontology' : ontology,
				'content_name' : item,
				'document_ID' : dkbID
			}
			triples.append('''<{graph}/document/{document_ID}/{content_name}> a <{ontology}#DocumentContent> .'''.format(**DATASETS))
			triples.append('''<{graph}/document/{document_ID}> <{ontology}#hasContent> <{graph}/document/{document_ID}/{content_name}> .'''.format(**DATASETS))
			for j, ds_item in enumerate(data['content'][item]):
				DATASAMPLES = {
					'graph': graph,
					'ontology': ontology,
					'document_ID' : dkbID,
					'content_name': item,
					'data_sample' : ds_item
				}
				#checking
				#print(ds_item)
				#print('{document_ID}_{content_name}'.format(**DATASAMPLES))
				triples.append('''<{graph}/datasample/{data_sample}> a <{ontology}#DataSample> .'''.format(**DATASAMPLES))
				triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsDataSample> <{graph}/datasample/{data_sample}> .'''.format(**DATASAMPLES))
		if item == 'plain_text':	
			PLAINTEXT = {
				'graph': graph,
				'ontology': ontology,
				'document_ID' : dkbID,
				'content_name': item,
				'data_taking_year' : data['content'][item]['data taking year'],
				'luminosity' : data['content'][item]['luminosity'].replace(' ','_'),
				'energy' : data['content'][item]['energy'].lower().replace(' ','')
			}
			triples.append('''<{graph}/document/{document_ID}/{content_name}> a <{ontology}#DocumentContent> .'''.format(**PLAINTEXT))
			triples.append('''<{graph}/document/{document_ID}> <{ontology}#hasContent> <{graph}/document/{document_ID}/{content_name}> .'''.format(**PLAINTEXT))
			#data taking year
			triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsDataTakingYear> "{data_taking_year}" .'''.format(**PLAINTEXT))
			#luminosity
			triples.append('''<{graph}/luminosity/{luminosity}> a <{ontology}#Luminosity> .'''.format(**PLAINTEXT))
			triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsLuminosity> <{graph}/luminosity/{luminosity}> .'''.format(**PLAINTEXT))
			triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsEnergy> <{ontology}#{energy}> .'''.format(**PLAINTEXT))
			#checking
			#print('{data_taking_year}_{content_name}_{luminosity}'.format(**PLAINTEXT))
			campaign = data['content'][item]['campaigns']
			#print len(campaign)
			if len(campaign) > 0:
				for j, pt_item in enumerate(campaign):
					PLAINTEXT["campaign"] = pt_item
					triples.append('''<{graph}/campaign/{campaign}> a <{ontology}#Campaign> .'''.format(**PLAINTEXT))
					triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsCampaign> <{graph}/campaign/{campaign}> .'''.format(**PLAINTEXT))
					del PLAINTEXT["campaign"]
			else:
				print ("No campaigns in this file.")
				
	#for i, item in enumerate(triples):
			#file.write(item)
	return triples
  
  
def main(args):
	stage = pyDKB.dataflow.stage.JSON2TTLProcessorStage()
	stage.process = process

	exit_code = 0
	try:
		stage.parse_args(args)
		stage.run()
	except (pyDKB.dataflow.DataflowException, RuntimeError), err:
		if str(err):
			sys.stderr.write("(ERROR) %s\n" % err)
		exit_code=1
	finally:
		stage.stop()

	exit(exit_code)
	
if __name__ == '__main__':
  main(sys.argv[1:])
