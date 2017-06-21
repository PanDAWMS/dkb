#!/usr/bin/env python

"""
Stage 054: document content metadata to TTL & SPARQL
_____________________

This is a test version for checking input.
"""

import sys
sys.path.append("../")

import pyDKB

#default graph and ontology
graph = "http://nosql.tpu.ru:8890/DAV/ATLAS"
ontology = "http://nosql.tpu.ru/ontology/ATLAS"

def process(msg):
  """
  Input message: JSON
  Output message: TTL
  """
  return pyDKB.dataflow.Message(pyDKB.dataflow.messageType.TTL)(msg.content())

#test function
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
			triples.append('''<{graph}/document/{document_ID}/{content_name}> a <{ontology}#DocumentContent> .\n'''.format(**DATASETS))
			triples.append('''<{graph}/document/{document_ID}> <{ontology}#hasContent> <{graph}/document/{document_ID}/{content_name}> .\n'''.format(**DATASETS))
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
				triples.append('''<{graph}/datasample/{data_sample}> a <{ontology}#DataSample> .\n'''.format(**DATASAMPLES))
				triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsDataSample> <{graph}/datasample/{data_sample}> .\n'''.format(**DATASAMPLES))
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
			triples.append('''<{graph}/document/{document_ID}/{content_name}> a <{ontology}#DocumentContent> .\n'''.format(**PLAINTEXT))
			triples.append('''<{graph}/document/{document_ID}> <{ontology}#hasContent> <{graph}/document/{document_ID}/{content_name}> .\n'''.format(**PLAINTEXT))
			#data taking year
			triples.append('''<{graph}/document_content/{document_ID}/{content_name}> <{ontology}#mentionsDataTakingYear> "{data_taking_year}" .\n'''.format(**PLAINTEXT))
			#luminosity
			triples.append('''<{graph}/luminosity/{luminosity}> a <{ontology}#Luminosity> .\n'''.format(**PLAINTEXT))
			triples.append('''<{graph}/document/{document_ID}/{content_name}> <ONTOLOGY#mentionsLuminosity> <{graph}/luminosity/{luminosity}> .\n'''.format(**PLAINTEXT))
			triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsEnergy> <{ontology}#{energy}> .\n'''.format(**PLAINTEXT))
			#checking
			#print('{data_taking_year}_{content_name}_{luminosity}'.format(**PLAINTEXT))
			campaign = data['content'][item]['campaigns']
			#print len(campaign)
			if len(campaign) > 0:
				for j, pt_item in enumerate(campaign):
					triples.append('''<{graph}/campaign/%s> a <{ontology}#Campaign> .\n'''.format(**PLAINTEXT) % (pt_item))
					triples.append('''<{graph}/document/{document_ID}/{content_name}> <{ontology}#mentionsCampaign> <{graph}/campaign/%s> .\n'''.format(**PLAINTEXT) % (pt_item))
			
			else:
				print ("No campaigns in this file.")
				
	#for i, item in enumerate(triples):
			#file.write(item)
	return triples
  
  
def main(args):
	stage = pyDKB.dataflow.stage.JSON2TTLProcessorStage()
	stage.process = process

	stage.parse_args(args)
	#stage.run()
	data_input = stage.input()
	
	for f in data_input:
		triples = doc_content_triples(f.content())
		
		#this code should be changed
		output_data = open("doc_content.ttl", 'w')
		
		for i, item in enumerate(triples):
			output_data.write(item)
	
		output_data.close()
  

if __name__ == '__main__':
  main(sys.argv[1:])
