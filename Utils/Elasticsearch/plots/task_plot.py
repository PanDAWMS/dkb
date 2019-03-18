# coding: utf-8
import numpy as np
import scipy as sp
import pandas as pd
import matplotlib as mpl
from matplotlib import pyplot as plt
import matplotlib.dates as md
import json
import datetime


with open("g:/task-steps-gantt-r.json") as infile:
    rawdata = json.load(infile)


# ### Препроцессинг данных
# Для построения графика в виде stacked bar, необходимо обеспечить, чтобы у всех наборов данных была одинаковая ось X.
# Для этого нужно собрать timestamp всех наборов данных в один set, превратить его в list, и отсортировать по возрастанию. Это даст нам все timestamp с гарантией, что ни один не пропущен.
# Затем, формируются новые наборы данных: первый параметр - timestamp, второй - количество задач данного типа на данный timestamp (если задач нет, ставим 0)
# Это гарантирует нам строгое совпадение оси X у всех наборов данных.
# Сначала извлекаются данные из json:
data = rawdata['aggregations']['stages']['buckets']


# Затем подготавливаются наборы данных:
# Выделяются данные по типам.
prepared_sets = []
prepared_dicts = []
prepared_set_names = []

for bucket in data:
    task_type = bucket['key']
    doc_count = bucket['doc_count']
    data_points = [(datetime.datetime.fromtimestamp(data_point['key'] / 1e3), data_point['doc_count']) for data_point in bucket['gantt_data']['buckets']]
    prepared_sets.append((task_type, data_points))   
    data_dict = {datetime.datetime.fromtimestamp(data_point['key'] / 1e3): data_point['doc_count'] for data_point in bucket['gantt_data']['buckets']}
    prepared_dicts.append(data_dict)
    prepared_set_names.append(bucket['key'])


# Формируется ось X
x_axis_set = set()
for prepared_set in prepared_sets:
    for record in prepared_set[1]:
        x_axis_set.add(record[0])
x_axis = list(x_axis_set)
x_axis.sort()


# Создаются нормализованные наборы данных (с единой осью X):
normalized_datasets = []
for dataset in prepared_dicts:
    normalized_dataset = []
    for x in x_axis:
        if x in dataset:
            normalized_dataset.append(dataset[x])
        else:
            normalized_dataset.append(0)
    normalized_datasets.append(normalized_dataset)


# Теперь в normalized_datasets наборы данных одной длины и с одной осью X.
width = 0.4
plots = []

plt.figure(figsize=(20,15))

for i in range(0, len(prepared_set_names)):
    if i > 0:
        bottom_datasets = [sum(x) for x in zip(*normalized_datasets[0:i])]
        plot = plt.bar(x_axis, normalized_datasets[i], width, bottom=bottom_datasets)
    else:        
        plot = plt.bar(x_axis, normalized_datasets[i], width)
    plots.append(plot)

ax=plt.gca()
xfmt = md.DateFormatter('%Y-%m-%d %H:%M:%S')
ax.xaxis.set_major_formatter(xfmt)
plt.ylabel('doc_count')
plt.title('Tasks plot')
plt.legend(prepared_set_names)

plt.savefig("g:/fig.png")
plt.show()
plt.close()