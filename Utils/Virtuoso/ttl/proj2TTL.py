'''
INPUT:
graph(optional)
ontology(optional)
JSON file (for example, 'dataperiods14.json')

OUTPUT:
TTL file - dataperiods.ttl

The fragment of data sample:
<http://nosql.tpu.ru:8890/DAV/ATLAS/project/data10_7TeV> a <http://nosql.tpu.ru/ontology/ATLAS#Project> .
<http://nosql.tpu.ru:8890/DAV/ATLAS/project/data10_7TeV> <http://nosql.tpu.ru/ontology/ATLAS#hasDescription> 'unsqueezed stable beam data (beta*=10m): typical beam spot width in x and y is 50-60 microns.' .
<http://nosql.tpu.ru:8890/DAV/ATLAS/project/data10_7TeV> <http://nosql.tpu.ru/ontology/ATLAS#hasStatus> 'locked' .
<http://nosql.tpu.ru:8890/DAV/ATLAS/dataperiod/2010_A_2> a <http://nosql.tpu.ru/ontology/ATLAS#DataTakingPeriod> .
<http://nosql.tpu.ru:8890/DAV/ATLAS/dataperiod/2010_A_2> <http://nosql.tpu.ru/ontology/ATLAS#hasYear> <http://nosql.tpu.ru/ontology/ATLAS#2010> .
<http://nosql.tpu.ru:8890/DAV/ATLAS/dataperiod/2010_A_2> <http://nosql.tpu.ru/ontology/ATLAS#hasPeriod> <http://nosql.tpu.ru/ontology/ATLAS#A> .
<http://nosql.tpu.ru:8890/DAV/ATLAS/project/data10_7TeV> <http://nosql.tpu.ru/ontology/ATLAS#hasLevel> <http://nosql.tpu.ru/ontology/ATLAS#Level2> .
<http://nosql.tpu.ru:8890/DAV/ATLAS/project/data10_7TeV> <http://nosql.tpu.ru/ontology/ATLAS#hasDataTakingPeriod> <http://nosql.tpu.ru:8890/DAV/ATLAS/dataperiod/2010_A_2> .
etc.

NOTICE:
According to the task, any project name should be checked for the presence in the database.
This task requires an access to data in Virtuoso.

This script just get all data from file despite the data in Virtuoso.
------------------------------------------------------------------------------
This script remove newline symbols in 'description'.
Triples:
 {project_name} <{ontology}#hasDescription> '{description}'
 {project_name} <{ontology}#hasStatus> '{status}'
 might be non-unique.
 -----------------------------------------------------------------------------
 1. Run
 2. Change graph and/or ontology if it's necessary
 3. Choose JSON file to import data
 4. Get .ttl file
'''

import hashlib
import json
import os
import re
from tkinter.filedialog import askopenfilename

# default graph and ontology
graph = "http://nosql.tpu.ru:8890/DAV/ATLAS"
ontology = "http://nosql.tpu.ru/ontology/ATLAS"

# choose graph
print("Current graph: " + graph + "\n")
graph_answer = input("Would you like to choose another one? [Y/N] ")
if graph_answer.lower() in ['y', 'yes']:
	graph = input("Please, insert a graph: ")
print("\nCurrent graph: " + graph + "\n")

# choose ontology
print("Current ontology: " + ontology + "\n")
ontology_answer = input("Would you like to choose another one? [Y/N] ")
if ontology_answer.lower() in ['y', 'yes']:
	ontology = input("Please, insert an ontology: ")
print("\nCurrent ontology: " + ontology + "\n")

# path
chosen_path = os.path.normpath(askopenfilename())

# year
# year = '20'+ chosen_path.rstrip('.json')[len(chosen_path.rstrip('.json'))-2:len(chosen_path.rstrip('.json'))]

# a ttl document with default name
output_data = open("data_periods.ttl", 'w')

# input
with open(chosen_path) as data_file:
    input_data = json.load(data_file)
		
	
listProj = []
for i, item in enumerate(input_data):
		year = "20" + str(re.findall(r'\d+', input_data[i]['projectName'])[0])
		dataPeriod = "<%s/dataperiod/%s_%s_%s>" % (graph, year, input_data[i]['period'], input_data[i]['periodLevel'])
		# project name
		projectName = input_data[i]['projectName']
		# a subject in ontology
		project = "<%s/project/%s>" % (graph, input_data[i]['projectName'])
		# deleting of newline symbol from description
		description = input_data[i]['description'].replace("\n", " ")
		DATAPERIODS = {
                    'graph': graph,
			'ontology': ontology,
			'dataPeriod': dataPeriod,
			'project_name': project,
			'period': input_data[i]['period'],
			'periodLevel': input_data[i]['periodLevel'],
			'year': year,
			'description': description,
			'status': input_data[i]['status']
		}
		
		if not input_data[i]['projectName'] in listProj:
			tripleProject = '''{project_name} a <{ontology}#Project> .\n'''.format(**DATAPERIODS)
			listProj.append(input_data[i]['projectName'])
			output_data.write(tripleProject)
		
		tripleDescription = '''{project_name} <{ontology}#hasDescription> '{description}' .\n'''.format(**DATAPERIODS)
		output_data.write(tripleDescription)
		tripleStatus = '''{project_name} <{ontology}#hasStatus> '{status}' .\n'''.format(**DATAPERIODS)
		output_data.write(tripleStatus)
		tripleData = '''{dataPeriod} a <{ontology}#DataTakingPeriod> .\n'''.format(**DATAPERIODS)
		output_data.write(tripleData)
		tripleAttrYear = '''{dataPeriod} <{ontology}#hasYear> <{ontology}#{year}> .\n'''.format(**DATAPERIODS)
		output_data.write(tripleAttrYear)
		tripleAttrPeriod = '''{dataPeriod} <{ontology}#hasPeriod> <{ontology}#{period}> .\n'''.format(**DATAPERIODS)
		output_data.write(tripleAttrPeriod)
		tripleAttrLevel = '''{project_name} <{ontology}#hasLevel> <{ontology}#Level{periodLevel}> .\n'''.format(**DATAPERIODS)
		output_data.write(tripleAttrLevel)
		mapping = '''{project_name} <{ontology}#hasDataTakingPeriod> {dataPeriod} .\n'''.format(**DATAPERIODS)
		output_data.write(mapping)
		
output_data.close()
