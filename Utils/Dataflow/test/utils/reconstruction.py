"""
Proof of concept script for DKB.

Reconstruct a task chain (task ids and connections between them) including a
given task by quering data from elasticsearch.

Authors:
  Vasilii Aulov (vasilii.aulov@cern.ch)
"""
import copy
import sys

import elasticsearch


INDEX = 'tasks_chain_data'


def es_connect():
    ''' Establish a connection to elasticsearch.

    TODO: take parameters from es config.

    :return: client with established elasticsearch connection
    :rtype: elasticsearch.client.Elasticsearch
    '''
    return elasticsearch.Elasticsearch(['localhost:9200'])


def get_chain_id(es, index, taskid):
    ''' Get chain_id by given taskid.

    :param es: elasticsearch client
    :type es: elasticsearch.client.Elasticsearch
    :param index: index containing tasks
    :type index: str
    :param taskid: taskid of the task to look for
    :type taskid: int or str

    :return: chain_id, or False if the task was not found
    :rtype: int or bool
    '''
    try:
        results = es.get(index=index, doc_type='_all', id=taskid,
                         _source=['chain_id'])
    except elasticsearch.exceptions.NotFoundError:
        return False
    return results['_source'].get('chain_id')


def get_chain_data(es, index, chain_id):
    ''' Get chain_data of all tasks that have given chain_id.

    :param es: elasticsearch client
    :type es: elasticsearch.client.Elasticsearch
    :param index: index containing tasks
    :type index: str
    :param chain_id: taskid of the chain's root
    :type chain_id: int or str

    :return: chain_data of all tasks in the chain, format is
             [
                 ...
                 [chain_id, other_taskid_1, other_taskid_2, ..., taskid],
                 ...
             ]
    :rtype: list
    '''
    srch = {'query': {'term': {'chain_id': chain_id}}}
    rtrn = []
    fr = 0
    sz = 2000
    while True:
        results = es.search(index=index, body=srch, from_=fr, size=sz,
                            _source=['chain_data'])
        if not results['hits']['hits']:
            break
        for hit in results['hits']['hits']:
            chain_data = hit['_source'].get('chain_data')
            if chain_data:
                rtrn.append(chain_data)
        fr += sz
    return rtrn


def construct_chain(data):
    ''' Build a chain from the list of its tasks' chain_data.

    Chain is a group of tasks where the first task is the root, and each next
    task has one of the previous ones as its parent (parent's output includes
    child's input).

    :param data: chain_data of all tasks in the chain
    :type data: list

    :return: constructed chain, format is
             {
                 ...
                 taskidN: [childN1_id, childN2_id, ...]
                 ...
             }
    :rtype: dict
    '''
    chain = {}
    # Order data from longest to shortest lists. Processing [1, 2, 3] is faster
    # than processing [1], then [1, 2] and then [1, 2, 3].
    # TODO: check this more extensively.
    data.sort(key=lambda cd: len(cd), reverse=True)
    for cd in data:
        cd.reverse()
        child = False
        for tid in cd:
            if tid in chain:
                if child:
                    chain[tid].append(child)
                break
            else:
                chain[tid] = []
                if child:
                    chain[tid].append(child)
                child = tid
    return chain


def check(chain, chain_data, task_id, indent=0):
    ''' Display the constructed chain.

    DEBUG FUNCTION
    Recursively display the constructed chain starting from given task_id (so,
    the chain's root should be used for getting a full chain). Also, find
    matching chain_data for each task and display it as well.

    :param chain: constructed chain
    :type chain: dict
    :param chain_data: chain_data of all tasks in the chain
    :type chain_data: list
    :param task_id: task from which to start
    :type task_id: int
    :param indent: recursion parameter, used for visualization and measuring
                   a (complete or partial) chain's length
    :type indent: int
    '''
    task_cd = False
    for cd in chain_data:
        if cd[-1] == task_id:
            task_cd = cd
            break
    print((' ' * indent + str(task_id), indent, task_cd))
    for tid in chain[task_id]:
        check(chain, chain_data, tid, indent + 1)


if __name__ == '__main__':
    es = es_connect()
    chain_id = get_chain_id(es, INDEX, sys.argv[1])
    if not chain_id:
        sys.exit(1)
    chain_data = get_chain_data(es, INDEX, chain_id)
    chain_data_original = copy.deepcopy(chain_data)  # DEBUG
    chain = construct_chain(chain_data)
    check(chain, chain_data_original, chain_id)  # DEBUG
