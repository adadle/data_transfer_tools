#coding: utf-8
"""

"""
from datetime import datetime

__author__ = 'Tony Lee'


class StringUtil(object):

    @staticmethod
    def formatvalue(source, typeinfo):
	try:
            if typeinfo == 'string':
            	return source

            if typeinfo == 'int':
            	return int(source)

            if typeinfo == 'double':
            	return float(source)

            if typeinfo == 'date':
                return datetime.strptime(source, '%Y-%m-%d')
        
            if typeinfo == 'timestamp':
                return datetime.strptime(source,'%Y-%m-%d %H:%M:%S')

            if typeinfo == 'boolean':
                return source.lower() in ('yes','t','true','1',1,'y') 
	except:
	    return None

if __name__ == '__main__':
    print StringUtil.formatvalue('2015-12-01 00:00:01','timestamp')
