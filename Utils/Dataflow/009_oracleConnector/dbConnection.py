"""
Classes representing connection to a database.
"""

import sys
from collections import defaultdict

try:
    import cx_Oracle
except ImportError:
    sys.stderr.write("(WARN) Failed to import module: cx_Oracle.\n")


class dbConnection(object):
    """ Interface class for DB Connection. """

    def __init__(self, **kwargs):
        """ Initialize the object. """
        raise NotImplementedError

    def establish(self):
        """ Establish the connection to DB.

        :return: True|False
        :rtype: bool
        """
        raise NotImplementedError

    def save_queries(self, queries):
        """ Save queries in hash.

        :type queries: dict
        """
        raise NotImplementedError

    def execute_saved(self, qname, **params):
        """ Execute query by name with parameters. """
        raise NotImplementedError

    def results(self, qname):
        """ Generator for iterator over query results. """
        raise NotImplementedError


class OracleConnection(dbConnection):
    """ Class representing connection to Oracle database. """

    dsn = None
    connection = None
    queries = defaultdict(dict)

    def __init__(self, dsn):
        """ Initialize DB connection with Data Source Name. """
        try:
            cx_Oracle
        except NameError:
            sys.stderr.write("(ERROR) Failed to create %s: cx_Oracle not"
                             " found." % self.__class__.__name__)
            raise RuntimeError

        self.dsn = dsn

    def establish(self):
        """ Establish connection to database. """
        try:
            self.connection = cx_Oracle.connect(self.dsn)
        except cx_Oracle.DatabaseError, err:
            sys.stderr.write("(ERROR) %s\n" % err)
            return False

        return True

    def save_queries(self, queries):
        """ Save queries.

        :param queries: hash with query parameters:
                        { qname: { 'file': filename } }
        :type queries: dict
        """
        if type(queries) != dict:
            raise TypeError("%s.save_queries(): parameter of type 'dict'"
                            " is expected; got: '%s'" % (
                                self.__class__.__name__, type(queries)))
        succeed = True
        for qname in queries:
            if type(queries[qname]) != dict:
                raise TypeError("%s.save_queries(): hash of 'dict' values"
                                " is expected; got: '%s'" % (
                                    self.__class__.__name__, type(queries)))
            if queries[qname].get('file'):
                if not self.save_query_file(qname, queries[qname]['file']):
                    sys.stderr.write("(WARN) Failed to save query '%s'\n"
                                     % qname)
                    succeed = False

        return succeed

    def save_query_file(self, qname, src_filename):
        """ Read query from file and save it in query hash. """
        try:
            f = open(src_filename)
            q = f.read().rstrip().rstrip(';')
            self.queries[qname]['query'] = q
        except IOError, err:
            sys.stderr.write("(ERROR) Failed to read query file: %s\n" % err)
            return False

        return True

    def query_cursor(self, qname):
        """ Get cursor for given query. """
        if not self.queries.get(qname):
            sys.stderr.write("(ERROR) Unknown query name: %s\n" % qname)
            return False

        if not self.queries[qname].get('cursor'):
            c = self.connection.cursor()
            try:
                c.prepare(self.queries[qname]['query'])
            except cx_Oracle.DatabaseError, err:
                sys.stderr.write("(ERROR) Failed to compile query '%s': %s"
                                 % (qname, err))
            self.queries[qname]['cursor'] = c

        return self.queries[qname]['cursor']

    def execute_saved(self, qname, **params):
        """ Execute query by name with parameters. """
        c = self.query_cursor(qname)
        if not c:
            return False

        c.execute(None, params)

        return True

    def results(self, qname, arraysize=1000, rows_as_dict=False):
        """ Generator for the iterator over executed query results. """
        c = self.query_cursor(qname)
        if not c:
            raise StopIteration

        if rows_as_dict and not self.queries[qname].get('columns') \
                and c.description:
            self.queries[qname]['columns'] = \
                [i[0].lower() for i in c.description]

        try:
            results = c.fetchmany(arraysize)
        except cx_Oracle.InterfaceError:
            sys.stderr.write("(ERROR) The query '%s' must be executed before"
                             " fetching any results.\n" % qname)
            raise StopIteration

        while results:
            for row in results:
                if rows_as_dict:
                    yield dict(zip(self.queries[qname]['columns'], row))
                else:
                    yield row
            results = c.fetchmany(arraysize)
