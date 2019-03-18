# coding: utf-8

import matplotlib as mpl
from matplotlib import pyplot as plt
import matplotlib.dates as md
import json
import datetime
import io


def get_common_x_axis(data):
    """
    Accepts a
    :param data:
    :return: A list of all X-values that are contained in any of input datasets
    """
    x_axis_set = set()
    for prepared_set in data:
        for record in prepared_set[1]:
            x_axis_set.add(record[0])
    x_axis = list(x_axis_set)
    x_axis.sort()
    return x_axis


def get_plot_data_from_json(json_data):
    """
    Extracts data to plot from a JSON output of Elasticsearch query.
    :param json_data: Results of ElasticSearch query. Should contain
    plots at json_data['aggregations']['stages']['buckets']
    :return: Tuple of data to plot that is accepted by the plot_events function. Tuple contains 3 elements,
    first element is a list of names for plot legend, second element is  a list of X-axis values in UNIX timestamp
    format, third element is a tuple of tuples of data points. Length of X-axis equals to length of each datapoint
    in 3rd element. Number of lists in 3rd element is equal to number of legend records in 1st element.
    """
    # Для построения графика в виде stacked bar, необходимо обеспечить, чтобы у всех наборов данных была
    # одинаковая ось X. Для этого нужно собрать timestamp всех наборов данных в один set, превратить его в list,
    # и отсортировать по возрастанию. Это даст нам все timestamp с гарантией, что ни один не пропущен.
    # Затем, формируются новые наборы данных: первый параметр - timestamp, второй - количество задач данного типа
    # на данный timestamp (если задач нет, то 0) Это гарантирует нам строгое совпадение оси X у всех наборов данных.
    # Сначала извлекаются данные из JSON:
    data = json_data['aggregations']['stages']['buckets']
    # Затем подготавливаются наборы данных:
    # Выделяются данные по типам.
    prepared_sets = []
    prepared_dicts = []
    prepared_set_names = []

    for bucket in data:
        task_type = bucket['key']
        doc_count = bucket['doc_count']
        data_points = [(datetime.datetime.fromtimestamp(data_point['key'] / 1e3), data_point['doc_count']) \
                       for data_point in bucket['gantt_data']['buckets']]
        prepared_sets.append((task_type, data_points))
        data_dict = {datetime.datetime.fromtimestamp(data_point['key'] / 1e3): data_point['doc_count'] \
                     for data_point in bucket['gantt_data']['buckets']}
        prepared_dicts.append(data_dict)
        prepared_set_names.append(bucket['key'])

    # Формируется ось X
    x_axis = get_common_x_axis(prepared_sets)

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
    return prepared_set_names, x_axis, normalized_datasets


def draw_dataset_plot(data):
    """
    Draws a plot based on datasets.
    :param data: Tuple of data to plot. Tuple contains 3 elements,
    first element is a list of names for plot legend, second element is  a list of X-axis values in datetime format,
    third element is a tuple of tuples of data points. Length of X-axis equals to length of each datapoint
    in 3rd element. Number of lists in 3rd element is equal to number of legend records in 1st element.
    :return: png-файл картинки в виде файла в памяти
    """
    width = 1
    plots = []
    prepared_set_names = data[0]
    x_axis = data[1]
    normalized_datasets = data[2]

    plt.figure(figsize=(20, 15))

    for i in range(0, len(prepared_set_names)):
        if i > 0:
            bottom_datasets = [sum(x) for x in zip(*normalized_datasets[0:i])]
            plot = plt.bar(x_axis, normalized_datasets[i], width, bottom=bottom_datasets)
        else:
            plot = plt.bar(x_axis, normalized_datasets[i], width)
        plots.append(plot)

    ax=plt.gca()
    xfmt = md.DateFormatter('%Y-%m-%d')
    ax.xaxis.set_major_formatter(xfmt)
    plt.ylabel('doc_count')
    plt.title('Tasks plot')
    plt.legend(prepared_set_names)
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    plt.close()
    buf.seek(0)
    return buf


if __name__ == '__main__':
    with open("g:/task-steps-gantt-r.json") as infile:
        rawdata = json.load(infile)
        data = get_plot_data_from_json(rawdata)
        image = draw_dataset_plot(data)
        print(image)
        with open("g:/test_fig.png", 'wb') as outfile:
            outfile.write(image.getvalue())
