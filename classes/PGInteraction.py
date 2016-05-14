# coco = common code
import logging
import psycopg2
import inspect
from datetime import datetime
from psycopg2 import errorcodes, extras
import sys
import os
import ConfigParser
import traceback


class PGInteraction (object):
    """
    """
    def __init__(self, dbname, host, user, password, schema):
        """
        """
        if not dbname or not host or not user or password is None:
            raise RuntimeError("%s request all __init__ arguments" % __name__)

        self.host     = host
        self.user     = user
        self.password = password
        self.dbname   = dbname

    def conn(self):
        """Open a connection, should be done right before time of insert
        """
        self.con = psycopg2.connect("dbname="+ self.dbname + " host=" + self.host +
                                    " user=" + self.user + " password=" + self.password)

    def batchOpen(self):
        """
        :return:
        """
        self.cur = self.con.cursor(cursor_factory=psycopg2.extras.DictCursor)


    def batchCommit(self):
        """
        """
        try:
            self.con.commit()
        except Exception as e:
            pgError =errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)
            #Logger().l("pgError")

    def fetch_sql(self, sql):
        """
        :param sql:
        :return:
        """
        try:
            self.cur.execute(sql)
            results = self.cur.fetchall()
        except Exception as e:
            pgError =errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)
        return results

    def exec_sql(self, sql):
        """
        :param sql:
        :return:
        """
        try:
            self.cur.execute(sql)
            results = self.cur.rowcount
        except Exception as e:
            pgError = errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)
        return results

    def bulkDictionaryInsert(self, table_name, col_dict, id_column=None):
        """
        """
        if len(col_dict) == 0:
            return

        placeholders = ', '.join(['%s'] * len(col_dict))
        columns = ', '.join(col_dict.keys())

        sql = "INSERT into %s ( %s ) VALUES ( %s )" % (table_name, columns, placeholders)

        if id_column is not None:
            sql = sql + " RETURNING " + id_column

        try:
            self.cur.execute(sql, col_dict.values())
            if id_column is not None:
                id_of_new_row = self.cur.fetchone()[0]
        except Exception as e:
            pgError = errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)
        if id_column is not None:
            return id_of_new_row
        else:
            return None


    def bulkPostCleanup(self,table_name):
        """
        """
        sql = """
          delete from {0}
          where etl_updated=0
          and nk in (select nk from {0} where etl_updated = 1);

          update {0} set etl_updated = 0 where etl_updated = 1;""" .format(table_name)

        try:
            self.cur.execute(sql)
        except Exception as e:
            pgError = errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)

    def getTableColumns(self, table_name):
        """
        :param table_name:
        :return:
        """
        schema = table_name.split('.')[0]
        table = table_name.split('.')[1]
        sql = """
          SELECT column_name
          FROM information_schema.columns
          WHERE table_schema = '%s' and table_name = '%s';""" % (schema, table)
        result = self.fetch_sql(sql)
        return result

    def setRowComplete(self, table_name, row_id):
        sql = """update {} set etl_updated = 1 where id = {}""".format(table_name, row_id)
        self.exec_sql(sql)
