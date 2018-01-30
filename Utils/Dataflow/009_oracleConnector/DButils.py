import exceptions
import time
import sys
import datetime
import traceback
import csv
import json

try:
    import cx_Oracle
except ImportError:
    print "****ERROR : DButils. Cannot import cx_Oracle"
    pass


def connectDEFT_DSN(dsn):
    connect = cx_Oracle.connect(dsn)
    cursor = connect.cursor()

    return connect, cursor


def connectDEFT(dbname, dbuser, pwd):
    connect = cx_Oracle.connect(dbuser, pwd, dbname)
    cursor = connect.cursor()

    return connect, cursor


def ResultIter(connection, query, arraysize=1000, rows_as_dict=False):
    """
    :param connection: Oracle connection
    :param query: query string
    :param arraysize: number of elements, processed at a time
    :param rows_as_dict:
    :return: return rows with headers as dicts (True), or as lists (False)
    """
    cursor = connection.cursor()
    cursor.execute(query)
    colnames = [i[0].lower() for i in cursor.description]
    # An iterator that uses fetchmany to keep memory usage down
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for row in results:
            if rows_as_dict:
                yield dict(zip(colnames, row))
            else:
                yield row


def OneByOneIter(connection, query, rows_as_dict=False):
    """
    We should remember that parsing LOB is a very heavy operation.
    Using fix_lob() function it's better to use fetchone() to avoid
    errors.
    https://bitbucket.org/anthony_tuininga/cx_oracle/issues/49/lob-object-documentation-clarification
    :param connection: Oracle connection
    :param query: query steing
    :param rows_as_dict: return rows with headers as dicts (True),
                         or as lists (False)
    :return:
    """
    cursor = connection.cursor()
    cursor.execute(query)
    colnames = [i[0].lower() for i in cursor.description]
    row = cursor.fetchone()
    while row:
        row = cursor.fetchone()
        if not row:
            break
        row = fix_lob(row)
        if rows_as_dict:
            yield dict(zip(colnames, row))
        else:
            yield row


def fix_lob(row):
    """
    This procedure is needed in case of using
    tables with LOB's values.
    AS usual LOB is JSON's.
    And we need to process JSON string as it was
    a set of columns.
    :param row:
    :return:
    """
    def convert(col):
        if isinstance(col, cx_Oracle.LOB):
            result = ''
            try:
                result = json.load(col)
            except Exception:
                result = str(col)
            return result
        else:
            return col
    return [convert(c) for c in row]


def QueryAll(connection, query):
    cursor = connection.cursor()
    # print query
    cursor.execute(query)

    dbrows = cursor.fetchall()
    cursor.close()

    return dbrows


def QueryToCSV(connection, query, filename, arraysize=100):
    cursor = connection.cursor()
    cursor.execute(query)
    with open(filename, 'wb') as fout:
        writer = csv.writer(fout)
        writer.writerow([i[0] for i in cursor.description])  # heading row
        while True:
            results = cursor.fetchmany(arraysize)
            if not results:
                break
            for row in results:
                row = fix_lob(row)
                writer.writerow(row)


def CSV2JSON(csv_file, json_file):
    csv_file_handler = open(csv_file, 'r')
    json_file_handler = open(json_file, 'w')
    reader = csv.DictReader(csv_file_handler)
    json_file_handler.write('[')
    for row in reader:
        json.dump(row, json_file_handler)
        json_file_handler.write(',')
        json_file_handler.write('\n')
    json_file_handler.write(']')


def QueryUpdate(connection, query):
    error = 0

    cursor = connection.cursor()

    try:
        cursor.execute(query)
    except Exception:
        error = 1
        print "Error : QueryUpdate - Oracle exception ; query ", query
        sys.exit(1)
    cursor.close()

    return error


def QueryCommit(connection):
    connection.commit()


def closeDB(pdb, cursor):
    cursor.close()
    pdb.close()
