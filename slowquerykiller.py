#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import time
import re
import datetime
import ConfigParser

script_path=os.path.split(os.path.realpath(__file__))[0]+'/config.ini'

cp=ConfigParser.ConfigParser()
cp.read(script_path)


server={}
try:
    server['host']=cp.get('server','host')
    server['port']=cp.get('server','port')
    server['user']=cp.get('server','user')
    server['password']=cp.get('server','password')
    server['log_path']=cp.get('server','log_path')
except :
    #raise ConfigParser.NoOptionError(e)
    print "config.ini ERROR: section [server] "
    exit()

moniter=[]
execute_time=999999
for it in cp.sections():
    name=it[:7]
    joiner=it[7:8]
    if name=="moniter" and ( joiner=="-" or joiner=="_"):
        print it
        try:
            pattern_text=cp.get(it,'pattern')
            timeout=int(cp.get(it,'timeout'))
            moniter.append({
                    'user':cp.get(it,'user'),
                    'host':cp.get(it,'host'),
                    'db':cp.get(it,'db'),
                    'command':cp.get(it,'command'),
                    'state':cp.get(it,'state'),
                    'pattern':re.compile(pattern_text,re.I),
                    'pattern_text':pattern_text,
                    'timeout':timeout,
                    'operate':cp.get(it,'operate'),
                })
            if timeout<execute_time:
                execute_time=timeout
        except:
            print "config.ini ERROR: section [moniter] "
            sys.exit(501)

print '.............'

print server
print moniter



sandbox=1   #调试沙盒






log_fp=open(server['log_path'],'a')



print "[mysql config] MySQLdb://%s:%s@%s %ss\nlog_path: %s"%(server['user'],server['password'],server['host'],server['port'],server['log_path'])



try:
    import MySQLdb
except ImportError,mesg:
    raise ImportError('\n\nMySQLdb not installed, exit\n\nyum install MySQL-python\npip install mysql-python')

print "running..."

try:
    db=MySQLdb.connect(server['host'],server['user'],server['password'])
except:
    print 'ERROR: db connect failed.\n  Check you configure in config.ini'
    sys.exit(502)

# TODO 这里计算一个执行时长的值，用于筛选超过该值的sql语句
#   设想：定义多个 moniter，对匹配的进程，执行日志记录或杀死等操作
#         这里的execute_time按其中time最小的一个moniter为准，用于筛选
cursor=db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
if sandbox==1:
    information_dbname="test"
else:
    information_dbname="information_schema"

count=0
while True:
    count+=1
    print "\n\n\n-----------------------------------\n## loop:%s"%(count)
    # The PROCESSLIST table is a nonstandard table. It was added in MySQL 5.1.7   
    #  - from https://dev.mysql.com/doc/refman/5.1/en/processlist-table.html
    sql="SELECT `ID`, `USER`, `HOST`, `DB`, `COMMAND`, `TIME`, `STATE`, `INFO` \
        FROM `%s`.`PROCESSLIST` \
        WHERE (`DB`!='information_schema' and `USER`!='root' \
          and `STATE`!='' and `STATE`!='Waiting for INSERT' and `STATE`!='Locked') \
          and  `TIME` > %s"%(information_dbname,execute_time)
    cursor.execute(sql)


    for row in cursor.fetchall():
        print '[process]',row['ID'], row['USER'], row['HOST'], row['DB'], row['COMMAND'], row['TIME'], row['STATE'],# row['INFO']
        if row['INFO']:
            q=row['INFO'].replace('\n','').replace('\r','')
            for it in moniter:
                print ' - testing:%s %ss, %s'%(it['db'],it['timeout'],it['pattern_text'])
                if (it['user']=='*' or it['user']==row['USER']) \
                    and ( it['db']=='*' or it['db']==row['DB']) \
                    and ( it['db']=='*' or it['db']==row['DB']) \
                    and ( it['host']=='*' or it['host']==row['HOST']) \
                    and ( it['command']=='*' or it['command']==row['COMMAND']) \
                    and ( it['state']=='*' or it['state']==row['STATE']) \
                    and row['TIME']>=it['timeout'] and it['pattern'].match(q):
                    #print "     match, to kill an log, id: %s  %s"%(row['ID'],sandbox)
                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    if it['operate']=='kill':
                        sql="kill %s"%(row['ID'])
                        if sandbox==1:
                            print "     sandbox kill"
                        else:
                            try:
                                cursor.execute(sql)
                            except:
                                pass
                    elif it['operate']=='log':
                        pass
                        print "     log"
                    else:
                        pass
                    line='#%s [%s] ID:%s %ss (%s@%s/%s) %s, %s\n%s\n\n'%(it['operate'],now,row['ID'],row['TIME'],row['USER'],row['HOST'],row['DB'],row['COMMAND'],row['STATE'],row['INFO'] )
                    log_fp.write(line)
                    break
                else:
                    print "     skip"

        #except:
        #    print "ERROR in rows loop ....."
    time.sleep(5)

cursor.close()



db.close()
log_fp.close()


