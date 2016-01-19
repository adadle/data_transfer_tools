# coding=utf-8
"""
read configs from yaml file,extract data from redshift to mongoDB.
see demo config file: ./example/mongo_loader_demo.yml

steps:
1.unload to s3 from redshift
2.s3 get to local
3.local parse mapping....
4.update mongo docs.

usage:
python redshift_to_mongo.py  example/mongo_loader_demo.yml --tmp_dir /tmp 
"""
import logging
import yaml
import sys
import os.path
import re
import gzip
import time
import subprocess


from etl_settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from db_utils import RedshiftUtil
from db_utils import MongoUtil
from stringutil import StringUtil
from psycopg2 import OperationalError, InternalError

__Author__ = 'TonyLee'

formatter = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.INFO)


class MongoLoader(object):

    fields_delim = ','
    fields_item_delim = ':'
    max_retry = 10

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def unload_from_redshift(self):
        """
        unload data from redshift to s3.
        :return: shell exit_code
        """

        sql = '''UNLOAD('%s') TO '%s' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
                ALLOWOVERWRITE GZIP PARALLEL OFF  delimiter '%s';
              ''' % (self.extract_sql, self.s3_path,  AWS_ACCESS_KEY_ID
                     , AWS_SECRET_ACCESS_KEY, self.delimiter)

        # something shit happens when unload exec.
        for x in xrange(MongoLoader.max_retry):
            try:
                    RedshiftUtil.execute(sql)
                    cmd = "s3cmd get --recursive --force %s %s " % (self.s3_path, self.local_file)
                    logging.info('download data from s3 to local: %s', cmd)

                    return subprocess.check_call(cmd, shell=True)
                    break
            except InternalError as iternalee:
               logging.error("Your sql run failed, info: ", iternalee)
               raise

            except:
                logging.error("Something error happens,we will have one more try.")
                time.sleep(60)
        else:
            logging.error("try %i times, still fail...", MongoLoader.max_retry)
            raise

    def upsert_to_mongo(self):
        """
        parse line by line to mongo docs, then upsert it.
        """
        fields_define_list = self.parse_fields_def_list()
        collect = MongoUtil.get_conn(self.mongo_host, int(self.mongo_port))[self.mongo_db][self.mongo_collection]
        logging.info("collection: %s", collect)

        with gzip.open(self.local_file, 'rb') as f:
            count = 0
            while True:
                line = f.readline()
                if not line or len(line) == 0:
                    break
                count += 1
                line_delim = self.delimiter
                expected_length = len(fields_define_list)
                actually_length = len(line.split(line_delim))
                if expected_length != actually_length:
                    logging.error("the lenth seprated by %s in line %s is not equals as you defined in %s, expected length %i, actually %i",line_delim, line, fields_define_list, expected_length, actually_length)
                query_doc, update_doc = self.generate_upsert_docs(line, fields_define_list)
                collect.update(query_doc, update_doc, upsert=True)
        logging.info("Totabl upsert docs: %i ",count)

    def parse_fields_def_list(self):
        """
        parse user field define as list. key and value are transformed in lower case.
        [(field_name,field_data_type),...]
        """
        ret_list = []
        for item in self.field_define.split(MongoLoader.fields_delim):
            print item
	    key, value = item.strip().split(MongoLoader.fields_item_delim)
            ret_list.append(
                            (key.strip().lower(), value.strip().lower())
                            )
        return ret_list

    def generate_upsert_docs(self, line, fields_define_list):
        """
        parse line to update doc
        """
        i = 0
        query_doc = {}
        update_doc = {}
        query_field_list = self.update_query.split(MongoLoader.fields_delim)
        line_list = line.split(self.delimiter)
        for item in fields_define_list:
            field = item[0]
            typeinfo = item[1]
            formatval = StringUtil.formatvalue(line_list[i], typeinfo)
            update_doc[field] = formatval
            if field in query_field_list:
                query_doc[field] = formatval
            i += 1
        update_doc = {"$set": update_doc}
        return query_doc, update_doc


def pair_left_args(args):
    """
    parse value passed in as dict .eg: --dt 2015-10-10 ==> {'dt':'2015-10-10'}
    :param args:
    :return:
    """
    separator = '--'
    delimiter = ' '
    user_args = {}
    for g in delimiter.join(args).split(separator):
        k, _, v = g.lstrip(separator).strip().partition(delimiter)
        if len(k) > 0:
            user_args[k] = v
    return user_args


def deal_with_conf_args(conf_args, user_args):
    """
    reset user define args with value passed in.
    :param conf_args:
    :param user_args:
    :return:
    """
    for k_conf in conf_args:
        for k_user in user_args:
            regex = '\$\{%s\}' % k_user
            conf_args[k_conf] = re.sub(regex, user_args[k_user], conf_args[k_conf])
    return conf_args


def main(argv):
    conf_file = argv[1]
    conf_args = {}
    require_keys = ['extract_sql', 's3_path', 'delimiter',
                    'local_file', 'mongo_host',
                    'mongo_port', 'mongo_db', 'mongo_collection']
    if conf_file is not None and os.path.exists(conf_file):
        conf_args = yaml.load(file(conf_file, 'r'))
        try:
            # check yaml required args are all required.
            if filter(lambda x: conf_args[x] is not None, require_keys):
                user_def_args = pair_left_args(argv[2:])
                logging.info('we receive your args: %s', user_def_args)
                # reset user define args if needed.
                if not user_def_args == {}:
                    conf_args = deal_with_conf_args(conf_args, user_def_args)
                logging.info('conf_args reseted with def args passed in: %s ', conf_args)
        except KeyError as e:
            logging.error('we need all the argments  %s', require_keys)
            raise KeyError('does not contain argument: %s in config file %s'
                           % (e.message, conf_file))
    loader = MongoLoader(**conf_args)
    logging.info('we will unload data from redshift to local path...')
    if loader.unload_from_redshift() == 0:
    	logging.info('read local data and upsert into mongo...')
    	loader.upsert_to_mongo()

if __name__ == '__main__':
    main(sys.argv)


