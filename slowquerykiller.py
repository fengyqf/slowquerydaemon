#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import re
import datetime
import ConfigParser

script_path=os.path.split(os.path.realpath(__file__))[0]+'/config.ini'

cp=ConfigParser.ConfigParser()
cp.read(script_path)

if cp.has_option('server','host'):
    HOST=cp.get('server','host')
else:
    HOST='127.0.0.1'
if cp.has_option('server','port'):
    PORT=int(cp.get('server','port'))
else:
    PORT=3306
if cp.has_option('server','user'):
    USER=cp.get('server','user')
else:
    USER='root'
if cp.has_option('server','password'):
    PASSWORD=cp.get('server','password')
else:
    PASSWORD=''
if cp.has_option('killer','pattern'):
    KILLER_PATTERN=cp.get('killer','pattern')
else:
    KILLER_PATTERN=''
if cp.has_option('server','max_time'):
    MAX_TIME=int(cp.get('server','max_time'))
else:
    MAX_TIME=10
if cp.has_option('server','log_path'):
    LOG_PATH=cp.get('server','log_path')
else:
    LOG_PATH=''


if not KILLER_PATTERN:
    print 'killer pattern not defined. exit'
    sys.exit(501)

print "[mysql config] MySQLdb://%s:%s@%s:%s\n[%s] %ss\nlog_path: %s"%(USER,PASSWORD,HOST,PORT,KILLER_PATTERN,MAX_TIME,LOG_PATH)

pattern=re.compile(KILLER_PATTERN,re.I)


try:
    import MySQLdb
except ImportError,mesg:
    raise ImportError('\n\nMySQLdb not installed, exit\n\nyum install MySQL-python\npip install mysql-python')

print "running..."

try:
    db=MySQLdb.connect(HOST,USER,PASSWORD)
except:
    print 'ERROR: db connect failed.\n  Check you configure in config.ini'
    sys.exit(502)

cursor=db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
sql="show full processlist"
cursor.execute(sql)


for row in cursor.fetchall():
    #print '[process]',row['Id'], row['User'], row['Host'], row['db'], row['Command'], row['Time'], row['State'], row['Info']
    #print '[process]',row['Id'], row['User'], row['Host'], row['db'], row['Command'], row['Time'], row['State']
    if row['Time'] > MAX_TIME and row['Info']:
        try:
            q=row['Info'].replace('\n','').replace('\r','')
            match=pattern.match(q)
            if match:
                sql="kill %s"%(row['Id'])
                cursor.execute(sql)

                if LOG_PATH:
                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    line='#kill log [%s]  %ss ID:%s (%s@%s/%s) %s %s\n%s\n\n'%(now,row['Time'],row['Id'],row['User'],row['Host'],row['db'],row['Command'],row['State'],row['Info'] )
                    try:
                        fp=open(LOG_PATH,'a')
                        fp.write(line)
                        fp.close()
                    except:
                        pass
                else:
                    print "LOG_PATH false"
            else:
                pass
        except:
            pass

cursor.close()



db.close()


