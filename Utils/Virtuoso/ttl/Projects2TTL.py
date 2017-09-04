'''
INPUT:
graph(optional)
ontology(optional)
.txt file (for example, 'Projects')

OUTPUT:
TTL file - projects.ttl

The fragment of data sample:
<http://nosql.tpu.ru:8890/DAV/ATLAS/project/cmccond> a <http://nosql.tpu.ru/ontology/ATLAS#Project> .
<http://nosql.tpu.ru:8890/DAV/ATLAS/project/cond09> a <http://nosql.tpu.ru/ontology/ATLAS#Project> .
<http://nosql.tpu.ru:8890/DAV/ATLAS/project/cond09_data> a <http://nosql.tpu.ru/ontology/ATLAS#Project> .
<http://nosql.tpu.ru:8890/DAV/ATLAS/project/cond09_mc> a <http://nosql.tpu.ru/ontology/ATLAS#Project> .
<http://nosql.tpu.ru:8890/DAV/ATLAS/project/cond09_test> a <http://nosql.tpu.ru/ontology/ATLAS#Project> .
<http://nosql.tpu.ru:8890/DAV/ATLAS/project/cond10> a <http://nosql.tpu.ru/ontology/ATLAS#Project> .
etc.
----------------------------------------------------------
 1. Run
 2. Change graph and/or ontology if it's necessary
 3. Choose .txt file to import data
 4. Get .ttl file
'''
import hashlib
import os
from tkinter.filedialog import askopenfilename


#default graph and ontology
graph = "http://nosql.tpu.ru:8890/DAV/ATLAS"
ontology = "http://nosql.tpu.ru/ontology/ATLAS"

#choose graph
print("Current graph: " + graph + "\n")
graph_answer = input("Would you like to choose another one? [Y/N] ")
if graph_answer.lower() in ['y', 'yes']:
	graph = input("Please, insert a graph: ")
print("\nCurrent graph: " + graph +"\n")    

#choose ontology
print("Current ontology: " + ontology +"\n")
ontology_answer = input("Would you like to choose another one? [Y/N] ")
if ontology_answer.lower() in ['y', 'yes']:
	ontology = input("Please, insert an ontology: ")
print("\nCurrent ontology: " + ontology +"\n")

#a ttl document with default name
output_object = open("projects.ttl", 'w')

#path to txt Projects
chosen_path = os.path.normpath(askopenfilename())

file_object = open(chosen_path, "r")
lines = file_object.readlines()
for i in lines:
	if not i.startswith("#"):
		project = "<%s/project/%s>" % (graph,i.rstrip('\n'))
		PROJECTS = {
			'graph': graph,
			'ontology': ontology,
			'project_name': project
		}
		triple = '''{project_name} a <{ontology}#Project> .\n'''.format(**PROJECTS)
		output_object.write(triple)

output_object.close()
file_object.close()
