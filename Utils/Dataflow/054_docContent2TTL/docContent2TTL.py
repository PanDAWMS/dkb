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
	dataset_suffix = "_datasets"
	
	try:
		dkbID = data['dkbID']
	except KeyError:
		sys.stderr.write("No dkbID.\n")
		return False

	triples = []
	for i, item in enumerate(data['content']):
		DEFAULT = {
			'graph' : graph,
			'ontology' : ontology,
			'content_name' : item,
			'document_ID' : dkbID
		}
		#for any type of dataset
		if item.endswith(dataset_suffix):
			triples.append('''<{graph}/document/{document_ID}/{content_name}> a <{ontology}#DocumentContent> .'''.format(**DEFAULT))
			triples.append('''<{graph}/document/{document_ID}> <{ontology}#hasContent> <{graph}/document/{document_ID}/{content_name}> .'''.format(**DEFAULT))
			for j, ds_item in enumerate(data['content'][item]):
				DATASAMPLES = {
					'data_sample' : ds_item
				}
				DATASAMPLES.update(DEFAULT)
				#checking
				#print(ds_item)
				#print('{document_ID}_{content_name}'.format(**DATASAMPLES))
				triples.append('''<{graph}/datasample/{data_sample}> a <{ontology}#DataSample> .'''.format(**DATASAMPLES))
				triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsDataSample> <{graph}/datasample/{data_sample}> .'''.format(**DATASAMPLES))
		if item == 'plain_text':
			
			try:
				data_taking_year = data['content'][item]['data taking year']
			except KeyError:
				data_taking_year = None
			
			try:
				luminosity = data['content'][item]['luminosity'].replace(' ','_')
			except (KeyError, AttributeError):
				luminosity = None
			
			try:
				energy = data['content'][item]['energy'].lower().replace(' ','')
			except (KeyError, AttributeError):
				energy = None
			
			PLAINTEXT = {
				'data_taking_year' : data_taking_year,
				'luminosity' : luminosity,
				'energy' : energy
			}
			PLAINTEXT.update(DEFAULT)
			triples.append('''<{graph}/document/{document_ID}/{content_name}> a <{ontology}#DocumentContent> .'''.format(**PLAINTEXT))
			triples.append('''<{graph}/document/{document_ID}> <{ontology}#hasContent> <{graph}/document/{document_ID}/{content_name}> .'''.format(**PLAINTEXT))
			
			if data_taking_year:
				#data taking year
				triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsDataTakingYear> "{data_taking_year}" .'''.format(**PLAINTEXT))
			
			if luminosity:
				#luminosity
				triples.append('''<{graph}/luminosity/{luminosity}> a <{ontology}#Luminosity> .'''.format(**PLAINTEXT))
				triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsLuminosity> <{graph}/luminosity/{luminosity}> .'''.format(**PLAINTEXT))
			
			if energy:
				#energy
				triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsEnergy> <{ontology}#{energy}> .'''.format(**PLAINTEXT))
			
			#checking
			#print('{data_taking_year}_{content_name}_{luminosity}'.format(**PLAINTEXT))
			try:
				campaign = data['content'][item]['campaigns']
				#print len(campaign)
				for j, pt_item in enumerate(campaign):
					PLAINTEXT["campaign"] = pt_item
					triples.append('''<{graph}/campaign/{campaign}> a <{ontology}#Campaign> .'''.format(**PLAINTEXT))
					triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsCampaign> <{graph}/campaign/{campaign}> .'''.format(**PLAINTEXT))
					del PLAINTEXT["campaign"]
			except KeyError:
				campaign = []
			if not campaign:
				sys.stderr.write("No campaigns in this file.\n")
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
