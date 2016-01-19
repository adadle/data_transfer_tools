# coding=utf-8
"""

"""

__Author__ = 'TonyLee'
import logging
from etl_settings import DB_USER

import psycopg2
from pymongo import MongoClient


formatter = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.INFO)

class RedshiftUtil(object):

    @classmethod
    def execute(cls, sql):
        with psycopg2.connect(**DB_USER) as conn:
            with conn.cursor() as cur:
                logging.info("run sql: %s", sql)
                cur.execute(sql)


class MongoUtil(object):

    @classmethod
    def get_conn(cls, host, port):
        return MongoClient(host, port)

if __name__ == '__main__':
    print RedshiftUtil.execute('')